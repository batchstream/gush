package gush

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
)

// functionName returns the name a flow or step function registers under, which is its
// bare Go name without package or receiver. A string is its own name, and anything that
// is not a function has no name.
func functionName(function any) string {
	name, ok := function.(string)
	if ok {
		return name
	}
	if function == nil {
		return ""
	}

	value := reflect.ValueOf(function)
	if value.Kind() != reflect.Func || value.Pointer() == 0 {
		return ""
	}
	runtimeFunction := runtime.FuncForPC(value.Pointer())
	if runtimeFunction == nil {
		return ""
	}

	elements := strings.Split(runtimeFunction.Name(), ".")
	// The compiler adds -fm to a method value, such as steps.Charge on a struct pointer.
	return strings.TrimSuffix(elements[len(elements)-1], "-fm")
}

// handlerName resolves a flow or step reference, reporting one that cannot be run.
func handlerName(kind string, handler any) (string, error) {
	name := functionName(handler)
	if name == "" {
		return "", fmt.Errorf("gush: %s must be a function or a registered name, got %T", kind, handler)
	}
	return name, nil
}
