package gush

import "unicode/utf8"

// maxFairKeyBytes is the longest fairness key the Temporal server accepts.
const maxFairKeyBytes = 64

// Priority is the scheduling priority of a flow or step. Higher priority runs first.
type Priority string

const (
	PriorityUrgent  Priority = "urgent"
	PriorityHigh    Priority = "high"
	PriorityMedium  Priority = "medium" // default
	PriorityLow     Priority = "low"
	PriorityTrivial Priority = "trivial"
)

// Int returns the Temporal priority key, from 1 for PriorityUrgent to 5 for
// PriorityTrivial. An unknown priority counts as PriorityMedium.
func (priority Priority) Int() int {
	switch priority {
	case PriorityUrgent:
		return 1
	case PriorityHigh:
		return 2
	case PriorityLow:
		return 4
	case PriorityTrivial:
		return 5
	}
	return 3
}

// NewPriority converts a Temporal priority key back to a Priority. An unknown key
// counts as PriorityMedium.
func NewPriority(key int) Priority {
	switch key {
	case 1:
		return PriorityUrgent
	case 2:
		return PriorityHigh
	case 4:
		return PriorityLow
	case 5:
		return PriorityTrivial
	}
	return PriorityMedium
}

// truncateFairKey cuts key to the length the server accepts, because a longer key fails
// the whole flow start or command rather than only losing fairness. It cuts between
// characters, since a string holding half a character cannot be marshaled.
func truncateFairKey(key string) string {
	if len(key) <= maxFairKeyBytes {
		return key
	}

	end := maxFairKeyBytes
	for end > 0 && !utf8.RuneStart(key[end]) {
		end--
	}
	return key[:end]
}
