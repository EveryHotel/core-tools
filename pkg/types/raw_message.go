package types

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
)

type RawMessage json.RawMessage

func (rm *RawMessage) Scan(value interface{}) error {
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
