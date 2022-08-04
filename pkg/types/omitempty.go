package types

import (
	"bytes"
	"encoding/json"
)

// Omitempty
//
// Example UnMarshal:
//
// Date
// {"field": "2020-02-23"} 	=> Omitempty{"2020-02-23", true}
// {"field": null } 		=> Omitempty{"", true}
// omitempty 				=> Omitempty{"", false}
//
// null.Int
// {"field": 6 } 	=> Omitempty{{6, true},  true}
// {"field": null } => Omitempty{{0, false}, true}
// omitempty 		=> Omitempty{{0, false}, false}
type Omitempty[T any] struct {
	Value T
	Valid bool
}

func (i *Omitempty[T]) UnmarshalJSON(data []byte) error {
	i.Valid = true

	if bytes.Equal(data, []byte("null")) {
		return nil
	}

	if err := json.Unmarshal(data, &i.Value); err != nil {
		return err
	}

	return nil
}
