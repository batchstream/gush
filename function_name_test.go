package gush

import (
	"strings"
	"testing"
)

type nameTestStruct struct{}

func (t *nameTestStruct) Method() {}

func nameTestFunction() {}

func TestFunctionName(t *testing.T) {
	var nilFunction func()
	receiver := &nameTestStruct{}

	tests := []struct {
		name     string
		input    any
		expected string
	}{
		{name: "nil", input: nil, expected: ""},
		{name: "string", input: "customFunction", expected: "customFunction"},
		{name: "not a function", input: 42, expected: ""},
		{name: "nil function", input: nilFunction, expected: ""},
		{name: "function", input: nameTestFunction, expected: "nameTestFunction"},
		{name: "anonymous function", input: func() {}, expected: "func1"},
		{name: "method expression", input: (*nameTestStruct).Method, expected: "Method"},
		{name: "method value", input: receiver.Method, expected: "Method"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := functionName(tt.input)
			if got != tt.expected {
				t.Errorf("functionName() = %q, expected %q", got, tt.expected)
			}
		})
	}
}

func genericTestStep[T any](*StepContext) (any, error) { return nil, nil }

type genericTestHandlers[T any] struct{}

func (genericTestHandlers[T]) Run(*StepContext) (any, error) { return nil, nil }

type flowNameForTest string

func TestFunctionNameOfATypeBuiltOnAString(t *testing.T) {
	if got := functionName(flowNameForTest("Named")); got != "Named" {
		t.Errorf("functionName() = %q", got)
	}
}

func TestFunctionNameOfGenericFunctions(t *testing.T) {
	if name := functionName(genericTestStep[int]); name != "genericTestStep" {
		t.Errorf("generic function = %q", name)
	}
	if name := functionName(genericTestStep[string]); name != "genericTestStep" {
		t.Errorf("another instantiation = %q", name)
	}

	var handlers genericTestHandlers[int]
	if name := functionName(handlers.Run); name != "Run" {
		t.Errorf("method of a generic type = %q", name)
	}
}

func TestHandlerNameReportsWhatCannotRun(t *testing.T) {
	_, err := handlerName("flow", 123)

	if err == nil || !strings.Contains(err.Error(), "flow must be a function") {
		t.Fatalf("handlerName() error = %v", err)
	}
}
