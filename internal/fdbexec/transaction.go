package fdbexec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"unsafe"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/futura-platform/futura/moment"
	"github.com/futura-platform/futura/privateencoding"
)

var byteOrder = binary.LittleEndian

// executionTransaction is a transaction over the database.
//
// The key layout is as follows:
//
//	call_order/("length")         -> uint64 little endian
//	call_order/(index)            -> encoded moment.Identity
//	memo_table/(encoded identity) -> encoded moment.Moment
//	durable/(key)                 -> value
type executionTransaction struct {
	fdb.Transaction
	executionReadTransaction
}

type executionReadTransaction struct {
	fdb.ReadTransaction
	memoTable      directory.DirectorySubspace
	callOrder      directory.DirectorySubspace
	durableObjects directory.DirectorySubspace
}

const callOrderLengthElement = "length"

func callOrderLengthKey(callOrder directory.DirectorySubspace) fdb.Key {
	return callOrder.Pack(tuple.Tuple{callOrderLengthElement})
}

func callOrderIndexKey(callOrder directory.DirectorySubspace, index int) fdb.Key {
	return callOrder.Pack(tuple.Tuple{index})
}

var ErrOutOfBounds = errors.New("index is out of bounds")

func (t *executionTransaction) AppendCallOrder(identity moment.Identity) {
	// set the value @ index l
	l := t.CallOrderLength()
	buf := bytes.NewBuffer(make([]byte, 0, unsafe.Sizeof(identity)))
	enc := privateencoding.NewEncoder[moment.Identity](buf)
	err := enc.Encode(identity)
	if err != nil {
		panic(err)
	}
	t.Set(callOrderIndexKey(t.callOrder, l), buf.Bytes())

	// increment the length
	b := make([]byte, 8)
	byteOrder.PutUint64(b, 1)
	t.Add(callOrderLengthKey(t.callOrder), b)
}

func (t *executionTransaction) SetCallOrderAt(index int, identity moment.Identity) {
	// technically we don't need a bounds check in production, assuming all Set calls are valid.
	if index < 0 || index >= t.CallOrderLength() {
		panic(ErrOutOfBounds)
	}

	buf := bytes.NewBuffer(make([]byte, 0, unsafe.Sizeof(identity)))
	enc := privateencoding.NewEncoder[moment.Identity](buf)
	err := enc.Encode(identity)
	if err != nil {
		panic(err)
	}
	t.Set(callOrderIndexKey(t.callOrder, index), buf.Bytes())
}

// TruncateCallOrderAt removes every call order entry after index, clamped to the call order's length.
func (t *executionTransaction) TruncateCallOrderAt(index int) {
	length := t.CallOrderLength()
	newLength := index + 1
	if newLength >= length {
		return
	}
	if newLength < 0 {
		panic(ErrOutOfBounds)
	}
	t.ClearRange(fdb.KeyRange{
		Begin: callOrderIndexKey(t.callOrder, newLength),
		End:   callOrderIndexKey(t.callOrder, length),
	})
	b := make([]byte, 8)
	byteOrder.PutUint64(b, uint64(newLength))
	t.Set(callOrderLengthKey(t.callOrder), b)
}

func momentTableKey(momentTable directory.DirectorySubspace, identity moment.Identity) fdb.Key {
	buf := bytes.NewBuffer(make([]byte, 0, unsafe.Sizeof(identity)))
	enc := privateencoding.NewEncoder[moment.Identity](buf)
	err := enc.Encode(identity)
	if err != nil {
		panic(err)
	}
	return momentTable.Pack(tuple.Tuple{buf.Bytes()})
}

func (t *executionTransaction) DeleteMoment(identity moment.Identity) {
	t.Clear(momentTableKey(t.memoTable, identity))
}

func (t *executionTransaction) SetMoment(identity moment.Identity, encodedMoment []byte) {
	t.Set(momentTableKey(t.memoTable, identity), encodedMoment)
}

func (t *executionTransaction) StoreDurable(key string, value []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("failed to store durable: %v", r)
		}
	}()
	t.Set(t.durableObjects.Pack(tuple.Tuple{key}), value)
	return nil
}

func (t *executionReadTransaction) CallOrderAt(index int) moment.Identity {
	b := t.Get(callOrderIndexKey(t.callOrder, index)).MustGet()
	if b == nil {
		panic(ErrOutOfBounds)
	}
	dec := privateencoding.NewDecoder[moment.Identity](
		bytes.NewReader(b),
	)
	id, err := dec.Decode()
	if err != nil {
		panic(err)
	}
	return id
}

func (t *executionReadTransaction) CallOrderLength() int {
	var length int64
	b := t.Get(callOrderLengthKey(t.callOrder)).MustGet()
	if len(b) == 0 {
		return 0
	}

	err := binary.Read(bytes.NewReader(b), byteOrder, &length)
	if err != nil {
		panic(err)
	}
	return int(length)
}

func (t *executionReadTransaction) GetMoment(identity moment.Identity) ([]byte, bool) {
	b := t.Get(momentTableKey(t.memoTable, identity)).MustGet()
	return b, b != nil
}

func (t *executionReadTransaction) HasMoment(identity moment.Identity) bool {
	// perform a range scan to check if the moment exists,
	// without loading the value of the identity.
	key := momentTableKey(t.memoTable, identity)
	iter := t.GetRange(fdb.KeyRange{
		Begin: key,
		End:   append(key, 0),
	}, fdb.RangeOptions{Limit: 1}).Iterator()

	if !iter.Advance() {
		return false // no keys found
	}

	kv, err := iter.Get()
	if err != nil {
		panic(err)
	}
	return string(kv.Key) == string(key)
}

// this is a potential failure point considering that the range scan is potentially unbounded...
// this will require rethinking in futura to be more thoughtful about potentially unbounded methods.
// TODO: fix this upstream in futura
func (t *executionReadTransaction) KnownMoments() iter.Seq[moment.Identity] {
	return func(yield func(moment.Identity) bool) {
		memoBegin, memoEnd := t.memoTable.FDBRangeKeys()
		memoRange := fdb.KeyRange{Begin: memoBegin, End: memoEnd}
		fdbIter := t.GetRange(memoRange, fdb.RangeOptions{}).Iterator()

		for fdbIter.Advance() {
			kv, err := fdbIter.Get()
			if err != nil {
				panic(err)
			}

			// Unpack the key to get the identity bytes
			unpacked, err := t.memoTable.Unpack(kv.Key)
			if err != nil {
				panic(err)
			}

			// The unpacked tuple contains the encoded identity bytes
			identityBytes := unpacked[0].([]byte)

			// Decode the identity
			dec := privateencoding.NewDecoder[moment.Identity](bytes.NewReader(identityBytes))
			identity, err := dec.Decode()
			if err != nil {
				panic(err)
			}

			if !yield(identity) {
				return
			}
		}
	}
}

func (t *executionReadTransaction) LoadDurable(key string) ([]byte, bool, error) {
	value, err := t.Get(t.durableObjects.Pack(tuple.Tuple{key})).Get()
	if err != nil {
		return nil, false, err
	}
	return value, value != nil, nil
}

var _ executiontype.Container = &executionTransaction{}
