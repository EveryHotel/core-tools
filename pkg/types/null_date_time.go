package types

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"time"
)

const NullTimeHMLayout = "15:04"
const nullTimeHMLayoutDB = "15:04:00.000000"

type NullTimeHM struct {
	Time  TimeHM
	Valid bool
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
