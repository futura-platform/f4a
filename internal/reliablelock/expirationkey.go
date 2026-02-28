package reliablelock

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func (l *Lock) writeExpirationKey(t fdb.Transaction, expiration time.Time) {
	expirationBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(expirationBytes, uint64(expiration.UnixMicro()))
	t.Set(l.holderExpirationKey(), expirationBytes)
}

func (l *Lock) readExpirationKey(t fdb.ReadTransaction) (time.Time, bool, error) {
	expirationBytes := t.Get(l.holderExpirationKey()).MustGet()
	if expirationBytes == nil {
		// key does not exist yet
		return time.Time{}, false, nil
	} else if len(expirationBytes) != 8 {
		return time.Time{}, false, errors.New("expiration key is not 8 bytes")
	}
	return time.UnixMicro(int64(binary.LittleEndian.Uint64(expirationBytes))), true, nil
}
