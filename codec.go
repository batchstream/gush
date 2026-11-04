package gush

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/klauspost/compress/zstd"
)

// The zstd coder's EncodeAll and DecodeAll are stateless and safe for concurrent use.
var (
	zstdDecoder, _ = zstd.NewReader(nil)
	zstdEncoder, _ = zstd.NewWriter(nil)
)

// Encode serializes a value the way flows and steps do for their input and output.
// []byte and string are used as they are, including through a pointer, a nil value or
// nil pointer encodes to nil, and everything else becomes JSON without HTML escaping.
//
// A string type of your own is not a string here, so it becomes JSON with its quotes. Read
// a value back as the type it was written.
func Encode(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}

	reflected := reflect.ValueOf(value)
	if reflected.Kind() == reflect.Pointer && reflected.IsNil() {
		return nil, nil
	}

	switch typed := value.(type) {
	case []byte:
		return typed, nil
	case *[]byte:
		return *typed, nil
	case string:
		return []byte(typed), nil
	case *string:
		return []byte(*typed), nil
	case *any:
		return Encode(*typed)
	}

	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(value)
	if err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buffer.Bytes(), []byte("\n")), nil
}

// Decode deserializes data produced by Encode into target. *string and *[]byte receive
// the bytes as they are, empty data zeroes target, and everything else is decoded as JSON.
// *any receives what the JSON holds, or a string when the data is not JSON.
func Decode(data []byte, target any) error {
	if len(data) == 0 {
		setToZeroValue(target)
		return nil
	}

	switch typed := target.(type) {
	case *string:
		*typed = string(data)
		return nil
	case *[]byte:
		*typed = bytes.Clone(data)
		return nil
	case *any:
		if !json.Valid(data) {
			*typed = string(data)
			return nil
		}
	}
	return json.Unmarshal(data, target)
}

// setToZeroValue zeroes the value target points to, leaving unexported struct fields alone.
func setToZeroValue(target any) {
	if target == nil {
		return
	}

	reflected := reflect.ValueOf(target)
	if reflected.Kind() != reflect.Pointer || reflected.IsNil() {
		return
	}
	setToZeroRecursive(reflected.Elem())
}

func setToZeroRecursive(value reflect.Value) {
	if !value.CanSet() {
		return
	}

	if value.Kind() != reflect.Struct {
		value.SetZero()
		return
	}
	for i := range value.NumField() {
		setToZeroRecursive(value.Field(i))
	}
}

// EncodePayload is the wire form of a flow or step input or output: Encode, then zstd.
// Code that works below Gush needs it, such as a test that starts a flow through the SDK's
// testsuite.
func EncodePayload(value any) ([]byte, error) {
	encoded, err := Encode(value)
	if err != nil {
		return nil, err
	}
	return zstdCompress(encoded), nil
}

// DecodePayload reverses EncodePayload. An empty payload or nil target leaves target alone.
func DecodePayload(payload []byte, target any) error {
	decompressed, err := zstdDecompress(payload)
	if err != nil {
		return err
	}
	if len(decompressed) == 0 || target == nil {
		return nil
	}
	return Decode(decompressed, target)
}

func zstdCompress(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	return zstdEncoder.EncodeAll(src, make([]byte, 0, len(src)))
}

func zstdDecompress(src []byte) ([]byte, error) {
	if len(src) == 0 {
		return nil, nil
	}
	decompressed, err := zstdDecoder.DecodeAll(src, nil)
	if err != nil {
		return nil, fmt.Errorf("gush: decompress payload: %w", err)
	}
	return decompressed, nil
}

func encodeResumeToken(token []byte) string {
	return base64.StdEncoding.EncodeToString(token)
}

func decodeResumeToken(token string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(token)
}
