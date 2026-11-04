package gush_test

import (
	"testing"

	"github.com/batchstream/gush"
)

func TestPriorityConstants(t *testing.T) {
	tests := []struct {
		name     string
		priority gush.Priority
		expected int
	}{
		{"PriorityUrgent", gush.PriorityUrgent, 1},
		{"PriorityHigh", gush.PriorityHigh, 2},
		{"PriorityMedium", gush.PriorityMedium, 3},
		{"PriorityLow", gush.PriorityLow, 4},
		{"PriorityTrivial", gush.PriorityTrivial, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.priority.Int(); got != tt.expected {
				t.Errorf("%s.Int() = %v, want %v", tt.name, got, tt.expected)
			}
		})
	}
}

func TestNewPriority(t *testing.T) {
	tests := []struct {
		name     string
		input    int
		expected gush.Priority
	}{
		{"Urgent", 1, gush.PriorityUrgent},
		{"High", 2, gush.PriorityHigh},
		{"Medium", 3, gush.PriorityMedium},
		{"Low", 4, gush.PriorityLow},
		{"Trivial", 5, gush.PriorityTrivial},
		{"InvalidHigh", 0, gush.PriorityMedium},
		{"InvalidLow", 10, gush.PriorityMedium},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := gush.NewPriority(tt.input); got != tt.expected {
				t.Errorf("NewPriority(%d) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestPriorityString(t *testing.T) {
	tests := []struct {
		name     string
		priority gush.Priority
		expected string
	}{
		{"PriorityUrgent", gush.PriorityUrgent, "urgent"},
		{"PriorityHigh", gush.PriorityHigh, "high"},
		{"PriorityMedium", gush.PriorityMedium, "medium"},
		{"PriorityLow", gush.PriorityLow, "low"},
		{"PriorityTrivial", gush.PriorityTrivial, "trivial"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := string(tt.priority); got != tt.expected {
				t.Errorf("%s.String() = %v, want %v", tt.name, got, tt.expected)
			}
		})
	}
}

func TestPriorityComparison(t *testing.T) {
	// Test that higher priority (lower number) has lower integer values
	if gush.PriorityUrgent.Int() >= gush.PriorityHigh.Int() {
		t.Error("PriorityUrgent should have lower integer value than PriorityHigh")
	}
	if gush.PriorityHigh.Int() >= gush.PriorityMedium.Int() {
		t.Error("PriorityHigh should have lower integer value than PriorityMedium")
	}
	if gush.PriorityMedium.Int() >= gush.PriorityLow.Int() {
		t.Error("PriorityMedium should have lower integer value than PriorityLow")
	}
	if gush.PriorityLow.Int() >= gush.PriorityTrivial.Int() {
		t.Error("PriorityLow should have lower integer value than PriorityTrivial")
	}
}

func TestPriorityRoundTrip(t *testing.T) {
	priorities := []gush.Priority{
		gush.PriorityUrgent,
		gush.PriorityHigh,
		gush.PriorityMedium,
		gush.PriorityLow,
		gush.PriorityTrivial,
	}

	for _, p := range priorities {
		t.Run(string(p), func(t *testing.T) {
			intVal := p.Int()
			converted := gush.NewPriority(intVal)
			if converted != p {
				t.Errorf("Round trip failed: %v -> %d -> %v", p, intVal, converted)
			}
		})
	}
}
