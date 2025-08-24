package bitemporal

import (
	"errors"
	"time"
)

// AsTime attempts to convert a few common date formats to a time.Time
// This exists purely for developer laziness
func AsTime(s string) time.Time {
	layouts := []string{
		time.DateTime,
		time.DateOnly,
		"2006-01-02 15:04:05-07:00",
	}

	for _, layout := range layouts {
		t, err := time.Parse(layout, s)
		if err == nil {
			return t
		}
	}

	panic(errors.New(s))
}
