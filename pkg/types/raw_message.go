package types

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
)

type RawMessage json.RawMessage

func (rm *RawMessage) Scan(value interface{}) error {
	if value == nil {
		return nil
	}

	buf, ok := value.([]byte)
	if !ok {
		return errors.New("couldn't parse to bytes")
	}

	*rm = append((*rm)[0:0], buf...)

	return nil
}

func (rm RawMessage) Value() (driver.Value, error) {
	return string(rm), nil
}

// MarshalJSON Реализация скопирована из оригинального RawMessage
func (m RawMessage) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte("null"), nil
	}
	return m, nil
}

// UnmarshalJSON Реализация скопирована из оригинального RawMessage
func (m *RawMessage) UnmarshalJSON(data []byte) error {
	if m == nil {
		return errors.New("json.RawMessage: UnmarshalJSON on nil pointer")
	}
	*m = append((*m)[0:0], data...)
	return nil
}
