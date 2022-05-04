package types

import (
	"fmt"
	"strings"
	"time"
)

const DateLayout = "2006-01-02"
const TimeLayout = "15:04:03"
const TimeHMLayout = "15:04"

type Date struct {
	time.Time
}

type Time struct {
	time.Time
}

type TimeHM struct {
	time.Time
}

func (c *Date) UnmarshalJSON(b []byte) (err error) {
	s := strings.Trim(string(b), `"`)
	if s == "null" {
		return
	}
	c.Time, err = time.Parse(DateLayout, s)
	return
}

func (c Date) MarshalJSON() ([]byte, error) {
	if c.Time.IsZero() {
		return []byte("null"), nil
	}
	return []byte(fmt.Sprintf(`"%s"`, c.Time.Format(DateLayout))), nil
}

func (c *Time) UnmarshalJSON(b []byte) (err error) {
	s := strings.Trim(string(b), `"`)
	if s == "null" {
		return
	}
	c.Time, err = time.Parse(TimeLayout, s)
	return
}

func (c Time) MarshalJSON() ([]byte, error) {
	if c.Time.IsZero() {
		return []byte("null"), nil
	}
	return []byte(fmt.Sprintf(`"%s"`, c.Time.Format(TimeLayout))), nil
}

func (c *TimeHM) UnmarshalJSON(b []byte) (err error) {
	s := strings.Trim(string(b), `"`)
	if s == "null" {
		return
	}
	c.Time, err = time.Parse(TimeHMLayout, s)
	return
}

func (c TimeHM) MarshalJSON() ([]byte, error) {
	if c.Time.IsZero() {
		return []byte("null"), nil
	}
	return []byte(fmt.Sprintf(`"%s"`, c.Time.Format(TimeHMLayout))), nil
}
