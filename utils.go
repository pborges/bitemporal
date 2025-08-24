package bitemporal

import (
	"errors"
	"strings"
	"time"
)

// AsTime attempts to convert a few common date formats to a time.Time
// This exists purely for developer laziness
func AsTime(s string) time.Time {
	layout := time.DateTime
	if !strings.Contains(s, " ") {
		layout = time.DateOnly
	}

	t, err := time.Parse(layout, strings.TrimSpace(s))
	if err != nil {
		panic(err)
	}
	if t.IsZero() {
		panic(errors.New("time is zero"))
	}
	return t
}
