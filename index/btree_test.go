package index

import (
	"fdb/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBtree_Put(t *testing.T) {
	bt := NewBtree()
	res1 := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	assert.Nil(t, res1)
	res2 := bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    1,
		Offset: 2,
	})
	assert.Nil(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    1,
		Offset: 2,
	})
	assert.NotNil(t, res3)
}

func TestBtree_Get(t *testing.T) {
	bt := NewBtree()
	res1 := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	assert.Nil(t, res1)
	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)
	res2 := bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    1,
		Offset: 2,
	})
	assert.Nil(t, res2)
	pos2 := bt.Get([]byte("a"))
	t.Log(pos2)
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(2), pos2.Offset)
}

func TestBtree_Delete(t *testing.T) {
	bt := NewBtree()
	res1 := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	assert.Nil(t, res1)
	res2, ok := bt.Delete(nil)
	assert.True(t, ok)
	assert.NotNil(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    1,
		Offset: 2,
	})
	assert.Nil(t, res3)
	del2, ok := bt.Delete([]byte("a"))
	t.Log(del2)
	assert.Equal(t, del2.Fid, uint32(1))
	assert.Equal(t, del2.Offset, int64(2))
	assert.NotNil(t, del2)
	assert.True(t, ok)
}
