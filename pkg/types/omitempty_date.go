package types

import (
	"bytes"
	"encoding/json"
)

// OmitemptyDate
//
// UnMarshal:
//
// * "2020-02-23" = ("2020-02-23", true)
//
// * null = ("", true)
//
// * omitempty = ("", false)

type OmitemptyDate struct {
	Date  Date
	Valid bool
}

func (i *OmitemptyDate) UnmarshalJSON(data []byte) error {
	i.Valid = true

	if bytes.Equal(data, []byte("null")) {
		return nil
	}

	if err := json.Unmarshal(data, &i.Date); err != nil {
		return err
	}

	return nil
}
