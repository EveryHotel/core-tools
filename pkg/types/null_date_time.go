package types

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"time"
)

const NullDateLayout = "2006-01-02"
const NullTimeHMLayout = "15:04"
const NullTimeLayout = "15:04:05"
const NullDateTimeLayout = "2006-01-02 15:04:05"
const nullTimeLayoutDB = "15:04:00.000000"
const nullTimeHMLayoutDB = "15:04:00.000000"

type NullTime struct {
	Time  Time
	Valid bool
}

type NullTimeHM struct {
	Time  TimeHM
	Valid bool
}

type NullDate struct {
	Time  Date
	Valid bool
}

type NullDateTime struct {
	Time  DateTime
	Valid bool
}

func (t NullTime) MarshalJSON() ([]byte, error) {
	if !t.Valid {
		return []byte("null"), nil
	}
	return t.Time.MarshalJSON()
}

func (t *NullTime) UnmarshalJSON(data []byte) error {
	if bytes.Equal(data, []byte("null")) {
		t.Valid = false
		return nil
	}

	if err := json.Unmarshal(data, &t.Time); err != nil {
		return err
	}

	t.Valid = true
	return nil
}

func (t *NullTime) Scan(v interface{}) error {

	if v == nil {
		t.Time.Time, t.Valid = time.Time{}, false
		return nil
	}

	parsed, err := time.Parse(nullTimeLayoutDB, v.(string))
	if err != nil {
		return err
	}

	t.Time.Time, t.Valid = parsed, true

	return nil
}

func (t NullTime) Value() (driver.Value, error) {
	if t.Time.IsZero() {
		return nil, nil
	}

	return t.Time.UTC().Format(NullTimeLayout), nil
}

func (t NullTime) String() string {
	if t.Time.IsZero() {
		return ""
	}

	return t.Time.UTC().Format(NullTimeLayout)
}

func (t NullTimeHM) MarshalJSON() ([]byte, error) {
	if !t.Valid {
		return []byte("null"), nil
	}
	return t.Time.MarshalJSON()
}

func (t *NullTimeHM) UnmarshalJSON(data []byte) error {
	if bytes.Equal(data, []byte("null")) {
		t.Valid = false
		return nil
	}

	if err := json.Unmarshal(data, &t.Time); err != nil {
		return err
	}

	t.Valid = true
	return nil
}

func (t *NullTimeHM) Scan(v interface{}) error {

	if v == nil {
		t.Time.Time, t.Valid = time.Time{}, false
		return nil
	}

	parsed, err := time.Parse(nullTimeHMLayoutDB, v.(string))
	if err != nil {
		return err
	}

	t.Time.Time, t.Valid = parsed, true

	return nil
}

func (t NullTimeHM) Value() (driver.Value, error) {
	if t.Time.IsZero() {
		return nil, nil
	}

	return t.Time.UTC().Format(NullTimeHMLayout), nil
}

func (t NullTimeHM) String() string {
	if t.Time.IsZero() {
		return ""
	}

	return t.Time.UTC().Format(NullTimeHMLayout)
}

func (t NullDate) MarshalJSON() ([]byte, error) {
	if !t.Valid {
		return []byte("null"), nil
	}
	return t.Time.MarshalJSON()
}

func (t *NullDate) UnmarshalJSON(data []byte) error {
	if bytes.Equal(data, []byte("null")) {
		t.Valid = false
		return nil
	}

	if err := json.Unmarshal(data, &t.Time); err != nil {
		return err
	}

	t.Valid = true
	return nil
}

func (t *NullDate) Scan(v interface{}) error {
	if v == nil {
		t.Time.Time, t.Valid = time.Time{}, false
		return nil
	}

	t.Time.Time, t.Valid = v.(time.Time), true

	return nil
}

func (t NullDate) Value() (driver.Value, error) {
	if t.Time.IsZero() {
		return nil, nil
	}

	return t.Time.UTC().Format(NullDateLayout), nil
}

func (t NullDate) String() string {
	if t.Time.IsZero() {
		return ""
	}

	return t.Time.UTC().Format(NullDateLayout)
}

func (t NullDateTime) MarshalJSON() ([]byte, error) {
	if !t.Valid {
		return []byte("null"), nil
	}
	return t.Time.MarshalJSON()
}

func (t *NullDateTime) UnmarshalJSON(data []byte) error {
	if bytes.Equal(data, []byte("null")) {
		t.Valid = false
		return nil
	}

	if err := json.Unmarshal(data, &t.Time); err != nil {
		return err
	}

	t.Valid = true
	return nil
}

func (t *NullDateTime) Scan(v interface{}) error {
	if v == nil {
		t.Time.Time, t.Valid = time.Time{}, false
		return nil
	}

	t.Time.Time, t.Valid = v.(time.Time), true

	return nil
}

func (t NullDateTime) Value() (driver.Value, error) {
	if t.Time.IsZero() {
		return nil, nil
	}

	return t.Time.UTC().Format(NullDateTimeLayout), nil
}

func (t NullDateTime) String() string {
	if t.Time.IsZero() {
		return ""
	}

	return t.Time.UTC().Format(NullDateTimeLayout)
}
