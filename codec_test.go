package gush

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncode(t *testing.T) {
	t.Run("nil returns nil", func(t *testing.T) {
		result, err := Encode(nil)
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("[]byte returns as-is", func(t *testing.T) {
		input := []byte(`hello world`)
		result, err := Encode(input)
		require.NoError(t, err)
		require.Equal(t, input, result)
	})

	t.Run("string returns raw bytes without JSON quotes", func(t *testing.T) {
		result, err := Encode("hello")
		require.NoError(t, err)
		require.Equal(t, []byte("hello"), result)
	})

	t.Run("*string dereferences and returns raw bytes", func(t *testing.T) {
		s := "hello"
		result, err := Encode(&s)
		require.NoError(t, err)
		require.Equal(t, []byte("hello"), result)
	})

	t.Run("*[]byte dereferences and returns as-is", func(t *testing.T) {
		b := []byte(`raw bytes`)
		result, err := Encode(&b)
		require.NoError(t, err)
		require.Equal(t, b, result)
	})

	t.Run("struct marshals to JSON", func(t *testing.T) {
		type Person struct {
			Name string `json:"name"`
			Age  int    `json:"age"`
		}
		person := Person{Name: "John", Age: 30}
		result, err := Encode(person)
		require.NoError(t, err)
		require.Equal(t, `{"name":"John","age":30}`, string(result))
	})

	t.Run("map marshals to JSON", func(t *testing.T) {
		result, err := Encode(map[string]int{"a": 1})
		require.NoError(t, err)
		require.Equal(t, `{"a":1}`, string(result))
	})

	t.Run("HTML tags are not escaped", func(t *testing.T) {
		result, err := Encode(map[string]string{"url": "https://example.com?a=1&b=2"})
		require.NoError(t, err)
		require.Contains(t, string(result), "&")
		require.NotContains(t, string(result), `\u0026`)
	})

	t.Run("JSON output has no trailing newline", func(t *testing.T) {
		result, err := Encode(42)
		require.NoError(t, err)
		require.Equal(t, "42", string(result))
	})
}

type (
	codecStatus string
	codecBytes  []byte
)

// codecTextStatus chooses its own JSON form through its text methods.
type codecTextStatus string

func (status codecTextStatus) MarshalText() ([]byte, error) {
	return []byte("text:" + string(status)), nil
}

func (status *codecTextStatus) UnmarshalText(data []byte) error {
	*status = codecTextStatus(strings.TrimPrefix(string(data), "text:"))
	return nil
}

type (
	codecMarshalsJSON   string
	codecUnmarshalsJSON string
	codecMarshalsText   string
	codecUnmarshalsText string
)

func (codecMarshalsJSON) MarshalJSON() ([]byte, error) { return nil, nil }

func (*codecUnmarshalsJSON) UnmarshalJSON([]byte) error { return nil }

func (codecMarshalsText) MarshalText() ([]byte, error) { return nil, nil }

func (*codecUnmarshalsText) UnmarshalText([]byte) error { return nil }

func TestEncodeAndDecodeTypesBuiltOnStringsAndBytes(t *testing.T) {
	t.Run("a string type has no quotes", func(t *testing.T) {
		status := codecStatus("done")
		for _, value := range []any{status, &status} {
			raw, err := Encode(value)
			require.NoError(t, err)
			require.Equal(t, "done", string(raw))
		}
	})

	t.Run("a string type reads back as itself or as a string", func(t *testing.T) {
		raw, err := Encode(codecStatus("done"))
		require.NoError(t, err)

		var status codecStatus
		require.NoError(t, Decode(raw, &status))
		require.Equal(t, codecStatus("done"), status)

		var plain string
		require.NoError(t, Decode(raw, &plain))
		require.Equal(t, "done", plain)
	})

	t.Run("a string reads back as a string type", func(t *testing.T) {
		var status codecStatus
		require.NoError(t, Decode([]byte("done"), &status))
		require.Equal(t, codecStatus("done"), status)
	})

	t.Run("a bytes type is not base64", func(t *testing.T) {
		original := codecBytes("raw bytes")
		raw, err := Encode(original)
		require.NoError(t, err)
		require.Equal(t, "raw bytes", string(raw))

		var decoded codecBytes
		require.NoError(t, Decode(raw, &decoded))
		require.Equal(t, original, decoded)

		raw[0] = 'x'
		require.Equal(t, byte('r'), decoded[0], "Decode must copy the data")
	})

	t.Run("a type with text methods stays JSON", func(t *testing.T) {
		raw, err := Encode(codecTextStatus("done"))
		require.NoError(t, err)
		require.Equal(t, `"text:done"`, string(raw))

		var status codecTextStatus
		require.NoError(t, Decode(raw, &status))
		require.Equal(t, codecTextStatus("done"), status)
	})

	t.Run("any one conversion method keeps a type JSON", func(t *testing.T) {
		types := []reflect.Type{
			reflect.TypeFor[codecMarshalsJSON](),
			reflect.TypeFor[codecUnmarshalsJSON](),
			reflect.TypeFor[codecMarshalsText](),
			reflect.TypeFor[codecUnmarshalsText](),
		}
		for _, valueType := range types {
			require.False(t, usedAsIs(valueType), "%v", valueType)
		}
		require.True(t, usedAsIs(reflect.TypeFor[codecStatus]()))
	})

	t.Run("json.RawMessage stays JSON", func(t *testing.T) {
		raw, err := Encode(json.RawMessage(`{"a": 1}`))
		require.NoError(t, err)
		require.Equal(t, `{"a":1}`, string(raw))

		var message json.RawMessage
		require.NoError(t, Decode(raw, &message))
		require.Equal(t, `{"a":1}`, string(message))
	})

	t.Run("a nil pointer target is an error", func(t *testing.T) {
		var status *codecStatus
		require.Error(t, Decode([]byte("done"), status))

		var anything *any
		require.Error(t, Decode([]byte("not json"), anything))
	})
}

func TestDecode(t *testing.T) {
	t.Run("[]byte assigned directly", func(t *testing.T) {
		m := make([]byte, 0)
		err := Decode([]byte(`{"age":30,"name":"John"}`), &m)
		require.NoError(t, err)
		require.Equal(t, []byte(`{"age":30,"name":"John"}`), m)
	})

	t.Run("*[]byte assigned directly", func(t *testing.T) {
		m := new([]byte)
		err := Decode([]byte(`{"age":30,"name":"John"}`), m)
		require.NoError(t, err)

		raw, err := Encode(m)
		require.NoError(t, err)
		require.Equal(t, []byte(`{"age":30,"name":"John"}`), raw)
	})

	t.Run("*string assigned directly without JSON parsing", func(t *testing.T) {
		var s string
		err := Decode([]byte("hello world"), &s)
		require.NoError(t, err)
		require.Equal(t, "hello world", s)
	})

	t.Run("struct unmarshals from JSON", func(t *testing.T) {
		type Person struct {
			Name string `json:"name"`
			Age  int    `json:"age"`
		}
		var p Person
		err := Decode([]byte(`{"name":"John","age":30}`), &p)
		require.NoError(t, err)
		expected := Person{Name: "John", Age: 30}
		require.Equal(t, expected, p)
	})

	t.Run("nil data resets float to zero", func(t *testing.T) {
		m := float32(99)
		err := Decode(nil, &m)
		require.NoError(t, err)
		require.Equal(t, float32(0), m)
	})

	t.Run("empty data resets string to zero", func(t *testing.T) {
		s := "non-empty"
		err := Decode([]byte{}, &s)
		require.NoError(t, err)
		require.Equal(t, "", s)
	})

	t.Run("nil data resets int to zero", func(t *testing.T) {
		n := 99
		err := Decode(nil, &n)
		require.NoError(t, err)
		require.Equal(t, 0, n)
	})

	t.Run("nil data resets slice to nil", func(t *testing.T) {
		sl := []string{"a", "b"}
		err := Decode(nil, &sl)
		require.NoError(t, err)
		require.Nil(t, sl)
	})
}

func TestSetToZeroValue(t *testing.T) {
	t.Run("basic types", func(t *testing.T) {
		// Test string
		str := "hello"
		setToZeroValue(&str)
		require.Equal(t, "", str)

		// Test int
		num := 42
		setToZeroValue(&num)
		require.Equal(t, 0, num)

		// Test bool
		flag := true
		setToZeroValue(&flag)
		require.Equal(t, false, flag)

		// Test float
		f := 3.14
		setToZeroValue(&f)
		require.Equal(t, 0.0, f)
	})

	t.Run("pointer types", func(t *testing.T) {
		// Test string pointer
		str := "hello"
		strPtr := &str
		setToZeroValue(&strPtr)
		require.Nil(t, strPtr)

		// Test int pointer
		num := 42
		numPtr := &num
		setToZeroValue(&numPtr)
		require.Nil(t, numPtr)
	})

	t.Run("slice types", func(t *testing.T) {
		// Test string slice
		slice := []string{"a", "b", "c"}
		setToZeroValue(&slice)
		require.Nil(t, slice)

		// Test int slice
		intSlice := []int{1, 2, 3}
		setToZeroValue(&intSlice)
		require.Nil(t, intSlice)
	})

	t.Run("map types", func(t *testing.T) {
		// Test string map
		m := map[string]int{"a": 1, "b": 2}
		setToZeroValue(&m)
		require.Nil(t, m)
	})

	t.Run("struct types", func(t *testing.T) {
		type Person struct {
			Name   string
			Age    int
			Active bool
			Score  float64
		}

		person := Person{
			Name:   "John",
			Age:    30,
			Active: true,
			Score:  95.5,
		}

		setToZeroValue(&person)
		var zero Person
		require.Equal(t, zero, person)
	})

	t.Run("nested struct types", func(t *testing.T) {
		type Address struct {
			Street string
			City   string
		}

		type Person struct {
			Name     string
			Age      int
			Address  Address
			Address2 *Address
		}

		person := Person{
			Name: "John",
			Age:  30,
			Address: Address{
				Street: "123 Main St",
				City:   "New York",
			},
			Address2: &Address{
				Street: "456 Main St",
				City:   "Los Angeles",
			},
		}

		setToZeroValue(&person)
		var zero Person
		require.Equal(t, zero, person)
	})

	t.Run("struct with pointers", func(t *testing.T) {
		type Person struct {
			Name    string
			Age     int
			Address *string
			Tags    []string
		}

		address := "123 Main St"
		person := Person{
			Name:    "John",
			Age:     30,
			Address: &address,
			Tags:    []string{"tag1", "tag2"},
		}

		setToZeroValue(&person)
		expected := Person{}
		require.Equal(t, expected, person)
	})

	t.Run("nil target", func(t *testing.T) {
		// Should not panic
		setToZeroValue(nil)
	})

	t.Run("non-pointer target", func(t *testing.T) {
		// Should not panic
		str := "hello"
		setToZeroValue(str)
		require.Equal(t, "hello", str) // Should remain unchanged
	})
}

func TestEncodeNilPointerIsNil(t *testing.T) {
	var value *struct{ A int }

	result, err := Encode(value)

	require.NoError(t, err)
	require.Nil(t, result)
}

type pointerMarshaler struct{}

func (*pointerMarshaler) MarshalJSON() ([]byte, error) {
	return []byte(`"custom"`), nil
}

func TestEncodeHonorsPointerReceiverMarshaler(t *testing.T) {
	marshaler := &pointerMarshaler{}
	result, err := Encode(marshaler)

	require.NoError(t, err)
	require.Equal(t, `"custom"`, string(result))
}

func TestPayloadRoundTrip(t *testing.T) {
	input := map[string]int{"a": 1}

	payload, err := EncodePayload(input)
	require.NoError(t, err)
	require.NotEqual(t, `{"a":1}`, string(payload))

	var output map[string]int
	require.NoError(t, DecodePayload(payload, &output))
	require.Equal(t, input, output)
}

func TestDecodePayloadLeavesTargetAloneWhenEmpty(t *testing.T) {
	output := "unchanged"

	require.NoError(t, DecodePayload(nil, &output))
	require.Equal(t, "unchanged", output)
	require.NoError(t, DecodePayload(nil, nil))
}

func TestDecodePayloadRejectsCorruptData(t *testing.T) {
	var output string

	require.Error(t, DecodePayload([]byte("not zstd"), &output))
}

func TestResumeTokenRoundTrip(t *testing.T) {
	token := []byte{0, 1, 2, 250}

	decoded, err := decodeResumeToken(encodeResumeToken(token))

	require.NoError(t, err)
	require.Equal(t, token, decoded)
}

func TestEncodeFollowsAPointerToAny(t *testing.T) {
	var holder any = "abc"
	result, err := Encode(&holder)
	require.NoError(t, err)
	require.Equal(t, "abc", string(result))

	holder = []byte("raw")
	result, err = Encode(&holder)
	require.NoError(t, err)
	require.Equal(t, "raw", string(result))

	holder = map[string]int{"a": 1}
	result, err = Encode(&holder)
	require.NoError(t, err)
	require.Equal(t, `{"a":1}`, string(result))
}

func TestDecodeIntoAny(t *testing.T) {
	var output any

	require.NoError(t, Decode([]byte("hello"), &output))
	require.Equal(t, "hello", output)

	require.NoError(t, Decode([]byte(`{"a":1}`), &output))
	expected := map[string]any{"a": 1.0}
	require.Equal(t, expected, output)
}
