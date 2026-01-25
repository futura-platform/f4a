package execute

import (
	"encoding/json"
)

func NewJsonMarshaller[A, R any]() ExecutionMarshaller[A, R] {
	return &genericExecutionMarshaller[A, R]{
		unmarshalInput: func(data []byte) (A, error) {
			var input A
			err := json.Unmarshal(data, &input)
			return input, err
		},
		marshalOutput: func(data R) ([]byte, error) {
			return json.Marshal(data)
		},
	}
}
