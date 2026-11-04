package gush

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"strconv"

	"go.opentelemetry.io/otel/attribute"
)

// LogAttribute is a key and value on a log record. It is OpenTelemetry's
// attribute.KeyValue, so attribute.Int64Slice and the like work wherever one is accepted.
type LogAttribute = attribute.KeyValue

// Attr creates a log attribute. Strings, booleans, numbers and []byte keep their type, a
// nil value has no value, and anything else is recorded in its fmt.Sprint form.
func Attr(key string, value any) LogAttribute {
	switch typed := value.(type) {
	case nil:
		empty := attribute.KeyValue{Key: attribute.Key(key)}
		return empty
	case string:
		return attribute.String(key, typed)
	case bool:
		return attribute.Bool(key, typed)
	case []byte:
		return attribute.ByteSlice(key, typed)
	case int:
		return attribute.Int(key, typed)
	case int8:
		return attribute.Int64(key, int64(typed))
	case int16:
		return attribute.Int64(key, int64(typed))
	case int32:
		return attribute.Int64(key, int64(typed))
	case int64:
		return attribute.Int64(key, typed)
	case uint:
		return unsignedAttr(key, uint64(typed))
	case uint8:
		return attribute.Int64(key, int64(typed))
	case uint16:
		return attribute.Int64(key, int64(typed))
	case uint32:
		return attribute.Int64(key, int64(typed))
	case uint64:
		return unsignedAttr(key, typed)
	case float32:
		return attribute.Float64(key, float64(typed))
	case float64:
		return attribute.Float64(key, typed)
	}
	return attribute.String(key, fmt.Sprint(value))
}

// unsignedAttr keeps a value that overflows int64 readable by recording it as a string.
func unsignedAttr(key string, value uint64) LogAttribute {
	if value > math.MaxInt64 {
		return attribute.String(key, strconv.FormatUint(value, 10))
	}
	return attribute.Int64(key, int64(value))
}

// LogAttributes are the fields attached to every log record of a run.
type LogAttributes map[string]any

// UnmarshalJSON adds the attributes in data to these, keeping numbers exact, and null clears
// them. Plain JSON decoding would turn every number into a float64, which damages large IDs.
func (attributes *LogAttributes) UnmarshalJSON(data []byte) error {
	decoded, err := decodeLogAttributes(data)
	if err != nil {
		return err
	}
	if decoded == nil || *attributes == nil {
		*attributes = decoded
		return nil
	}
	maps.Copy(*attributes, decoded)
	return nil
}

// decodeLogAttributes reads attributes as they arrive from another worker. The SDK's own
// decoding would turn every number into a float64, which damages large IDs.
func decodeLogAttributes(raw json.RawMessage) (map[string]any, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()

	var attributes map[string]any
	err := decoder.Decode(&attributes)
	if err != nil {
		return nil, fmt.Errorf("gush: decode log attributes: %w", err)
	}
	resolveNumbers(attributes)
	return attributes, nil
}

// resolveNumbers replaces each json.Number with the narrowest of int64, uint64 and float64
// that holds it.
func resolveNumbers(attributes map[string]any) {
	for key, value := range attributes {
		number, ok := value.(json.Number)
		if !ok {
			continue
		}

		signed, err := number.Int64()
		if err == nil {
			attributes[key] = signed
			continue
		}
		unsigned, err := strconv.ParseUint(number.String(), 10, 64)
		if err == nil {
			attributes[key] = unsigned
			continue
		}
		float, err := number.Float64()
		if err == nil {
			attributes[key] = float
		}
	}
}
