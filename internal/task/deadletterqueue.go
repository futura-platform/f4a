package task

import (
	"github.com/futura-platform/f4a/internal/reliablequeue"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

const deadLetterQueueDirectoryName = "task_dead_letter_queue"

type deadLetterSerializer struct{}

func (deadLetterSerializer) Marshal(v string) []byte {
	return []byte(v)
}

func (deadLetterSerializer) Unmarshal(bytes []byte) (string, error) {
	return string(bytes), nil
}

func CreateOrOpenDeadLetterQueue(db dbutil.DbRoot) (reliablequeue.TFIFO[string], error) {
	return reliablequeue.CreateOrOpenTFIFO(db, []string{deadLetterQueueDirectoryName}, deadLetterSerializer{})
}
