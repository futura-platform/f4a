package execute

import (
	"fmt"

	taskv1 "github.com/futura-platform/f4a/internal/gen/task/v1"
	"google.golang.org/protobuf/proto"
)

// protoTaskResult carries a TaskResult across futura steps. Protobuf oneofs
// are not privateencoding-serializable directly, so BinaryMarshaler delegates
// to proto.Marshal instead.
type protoTaskResult struct {
	*taskv1.TaskResult
}

func newTaskResultSuccess(data []byte) protoTaskResult {
	r := &taskv1.TaskResult{}
	r.SetResult(data)
	return protoTaskResult{TaskResult: r}
}

func newTaskResultFailure(message string) protoTaskResult {
	r := &taskv1.TaskResult{}
	r.SetFailure(message)
	return protoTaskResult{TaskResult: r}
}

func (p protoTaskResult) MarshalBinary() ([]byte, error) {
	return proto.Marshal(p.TaskResult)
}

func (p *protoTaskResult) UnmarshalBinary(data []byte) error {
	p.TaskResult = &taskv1.TaskResult{}
	if err := proto.Unmarshal(data, p.TaskResult); err != nil {
		return fmt.Errorf("unmarshal task result: %w", err)
	}
	return nil
}
