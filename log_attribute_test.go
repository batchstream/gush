package gush_test

import (
	"math"
	"testing"

	"github.com/batchstream/gush"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
)

func TestAttr(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  attribute.Value
	}{
		{name: "string", value: "text", want: attribute.StringValue("text")},
		{name: "bool", value: true, want: attribute.BoolValue(true)},
		{name: "int", value: 7, want: attribute.Int64Value(7)},
		{name: "int8", value: int8(7), want: attribute.Int64Value(7)},
		{name: "int16", value: int16(7), want: attribute.Int64Value(7)},
		{name: "int32", value: int32(7), want: attribute.Int64Value(7)},
		{name: "int64", value: int64(7), want: attribute.Int64Value(7)},
		{name: "uint", value: uint(7), want: attribute.Int64Value(7)},
		{name: "uint8", value: uint8(7), want: attribute.Int64Value(7)},
		{name: "uint16", value: uint16(7), want: attribute.Int64Value(7)},
		{name: "uint32", value: uint32(7), want: attribute.Int64Value(7)},
		{name: "uint64", value: uint64(7), want: attribute.Int64Value(7)},
		{name: "uint64 past int64", value: uint64(math.MaxUint64), want: attribute.StringValue("18446744073709551615")},
		{name: "float32", value: float32(1.5), want: attribute.Float64Value(1.5)},
		{name: "float64", value: 2.5, want: attribute.Float64Value(2.5)},
		{name: "bytes", value: []byte("raw"), want: attribute.ByteSliceValue([]byte("raw"))},
		{name: "other types keep their text", value: []int{1, 2}, want: attribute.StringValue("[1 2]")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attr := gush.Attr("key", tt.value)

			assert.Equal(t, attribute.Key("key"), attr.Key)
			assert.Equal(t, tt.want, attr.Value)
		})
	}
}

func TestAttrNilHasNoValue(t *testing.T) {
	attr := gush.Attr("key", nil)

	assert.Equal(t, attribute.Key("key"), attr.Key)
	assert.Equal(t, attribute.INVALID, attr.Value.Type())
}
