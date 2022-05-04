package types

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"errors"
)

type NullRawMessage struct {
	RawMessage json.RawMessage
	Valid      bool
}

func (rm *NullRawMessage) Scan(value interface{}) error {
	if value == nil {
		rm.RawMessage, rm.Valid = json.RawMessage{}, false
		return nil
	}

	buf, ok := value.([]byte)
	if !ok {
		return errors.New("couldn't parse to bytes")
	}

	if bytes.Equal(buf, []byte("null")) {
		rm.RawMessage, rm.Valid = buf, false
		return nil
	}

	rm.RawMessage, rm.Valid = buf, true

	return nil
}

func (rm NullRawMessage) Value() (driver.Value, error) {
	if !rm.Valid {
		return nil, nil
	}

	return string(rm.RawMessage), nil
}

func (rm NullRawMessage) MarshalJSON() ([]byte, error) {
	if !rm.Valid {
		return []byte("null"), nil
	}

	return rm.RawMessage.MarshalJSON()
}

func NewNullRawMessage(rm json.RawMessage, valid bool) NullRawMessage {
	return NullRawMessage{
		RawMessage: rm,
		Valid:      valid,
	}
}

func NullRawMessageFrom(value interface{}) (NullRawMessage, error) {
	invalid := NewNullRawMessage(json.RawMessage{}, false)

	if value == nil {
		return invalid, nil
	}

	buf, err := json.Marshal(value)
	if err != nil {
		return invalid, err
	}

	if bytes.Equal(buf, []byte("null")) {
		return invalid, nil
	}

	return NewNullRawMessage(buf, true), nil
}
