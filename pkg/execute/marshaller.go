package execute

type ExecutionMarshaller[A, R any] interface {
	UnmarshalInput(data []byte) (A, error)
	MarshalOutput(data R) ([]byte, error)
}

type genericExecutionMarshaller[A, R any] struct {
	unmarshalInput func(data []byte) (A, error)
	marshalOutput  func(data R) ([]byte, error)
}

func (m *genericExecutionMarshaller[A, R]) UnmarshalInput(data []byte) (A, error) {
	return m.unmarshalInput(data)
}

func (m *genericExecutionMarshaller[A, R]) MarshalOutput(data R) ([]byte, error) {
	return m.marshalOutput(data)
}
