package gush

import (
	"bytes"
	"encoding"
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

// conversionMethods are the interfaces through which a type chooses its own JSON form.
var conversionMethods = []reflect.Type{
	reflect.TypeFor[json.Marshaler](),
	reflect.TypeFor[json.Unmarshaler](),
	reflect.TypeFor[encoding.TextMarshaler](),
	reflect.TypeFor[encoding.TextUnmarshaler](),
}

// usedAsIs reports whether values of a type are their own encoding: strings and byte
// slices, including types built on them, unless the type chooses its own JSON form.
func usedAsIs(valueType reflect.Type) bool {
	if valueType.Kind() != reflect.String && !isByteSlice(valueType) {
		return false
	}
	return !hasConversionMethods(valueType)
}

// isByteSlice reports whether a type is a slice of bytes. Elements with their own JSON or
// text form make it a list instead, as they do for encoding/json.
func isByteSlice(valueType reflect.Type) bool {
	if valueType.Kind() != reflect.Slice || valueType.Elem().Kind() != reflect.Uint8 {
		return false
	}
	return !hasConversionMethods(valueType.Elem())
}

func hasConversionMethods(valueType reflect.Type) bool {
	pointer := reflect.PointerTo(valueType)
	for _, methods := range conversionMethods {
		if pointer.Implements(methods) {
			return true
		}
	}
	return false
}

// Encode serializes a value the way flows and steps do for their input and output.
// Strings and byte slices are used as they are, through any number of pointers and including
// types built on them, unless the type has its own JSON or text methods. A nil value or nil
// pointer encodes to nil, and everything else becomes JSON without HTML escaping.
//
// Adding or removing those methods changes how a type built on a string or bytes encodes, so
// a flow in flight then reads its old value wrongly. Drain such flows first.
func Encode(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}

	reflected := reflect.ValueOf(value)
	for reflected.Kind() == reflect.Pointer {
		if reflected.IsNil() {
			return nil, nil
		}
		reflected = reflected.Elem()
	}
	if reflected.Kind() == reflect.Interface {
		return Encode(reflected.Interface())
	}

	if usedAsIs(reflected.Type()) {
		if reflected.Kind() == reflect.String {
			return []byte(reflected.String()), nil
		}
		return reflected.Bytes(), nil
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

// Decode deserializes data produced by Encode into target. A pointer to anything Encode uses
// as it is, through any number of pointers, receives the bytes as they are, empty data zeroes
// target, and everything else is decoded as JSON. *any receives what the JSON holds, or a
// string when the data is not JSON.
func Decode(data []byte, target any) error {
	if len(data) == 0 {
		setToZeroValue(target)
		return nil
	}

	typed, ok := target.(*any)
	if ok && typed != nil && !json.Valid(data) {
		*typed = string(data)
		return nil
	}

	destination, ok := rawDestination(target)
	if !ok {
		return json.Unmarshal(data, target)
	}
	if destination.Kind() == reflect.String {
		destination.SetString(string(data))
		return nil
	}
	destination.SetBytes(bytes.Clone(data))
	return nil
}

// rawDestination finds what receives data as it is when target leads to a type Encode uses
// as it is, allocating the pointers on the way, as encoding/json does for a nil pointer.
func rawDestination(target any) (destination reflect.Value, ok bool) {
	destination = reflect.ValueOf(target)
	if destination.Kind() != reflect.Pointer || destination.IsNil() {
		return destination, false
	}

	finalType := destination.Type().Elem()
	for finalType.Kind() == reflect.Pointer {
		finalType = finalType.Elem()
	}
	if !usedAsIs(finalType) {
		return destination, false
	}

	destination = destination.Elem()
	for destination.Kind() == reflect.Pointer {
		if destination.IsNil() {
			destination.Set(reflect.New(destination.Type().Elem()))
		}
		destination = destination.Elem()
	}
	return destination, true
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
