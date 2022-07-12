package types

import (
	"bytes"
	"encoding/json"

	"github.com/guregu/null"
)

// OmitemptyInt64
//
// UnMarshal:
//
// * 6 = ((6, true), true)
//
// * null = ((0, false), true)
//
// * omitempty = ((0, false), false)
type OmitemptyInt64 struct {
	NullInt null.Int
	Valid   bool
}

func (i *OmitemptyInt64) UnmarshalJSON(data []byte) error {
	i.Valid = true

	if bytes.Equal(data, []byte("null")) {
		return nil
	}

	if err := json.Unmarshal(data, &i.NullInt); err != nil {
		return err
	}

	return nil
}
