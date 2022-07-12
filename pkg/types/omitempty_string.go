package types

import (
	"bytes"
	"encoding/json"

	"github.com/guregu/null"
)

// OmitemptyString
//
// UnMarshal:
//
// * "word" = (("word", true), true)
//
// * null = (("", false), true)
//
// * omitempty = (("", false), false)
type OmitemptyString struct {
	NullString null.String
	Valid      bool
}

func (i *OmitemptyString) UnmarshalJSON(data []byte) error {
	i.Valid = true

	if bytes.Equal(data, []byte("null")) {
		return nil
	}

	if err := json.Unmarshal(data, &i.NullString); err != nil {
		return err
	}

	return nil
}
