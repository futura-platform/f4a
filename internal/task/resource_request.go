package task

import (
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	taskv1 "github.com/futura-platform/f4a/internal/gen/task/v1"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	"google.golang.org/protobuf/proto"
)

type resourceRequestSerializer struct{}

// Marshal implements dbutil.serializable.
func (s resourceRequestSerializer) Marshal(v *taskv1.TaskResourceRequest) []byte {
	if v == nil {
		return nil
	}
	bytes, err := proto.Marshal(v)
	if err != nil {
		panic(fmt.Errorf("marshal resource request: %w", err))
	}
	return bytes
}

// Unmarshal implements dbutil.serializable.
func (s resourceRequestSerializer) Unmarshal(bytes []byte) (*taskv1.TaskResourceRequest, error) {
	if bytes == nil {
		return nil, nil
	}
	v := &taskv1.TaskResourceRequest{}
	if err := proto.Unmarshal(bytes, v); err != nil {
		return nil, fmt.Errorf("unmarshal resource request: %w", err)
	}
	return v, nil
}

func (k TaskKey) ResourceRequest() dbutil.TypedKey[*taskv1.TaskResourceRequest] {
	return dbutil.NewTypedKey(
		k.d.Pack(tuple.Tuple{string(k.id), "resource_request"}),
		resourceRequestSerializer{},
	)
}
