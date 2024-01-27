package data

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	// 正常编码一条数据
	rec1 := &LogRecord{
		Key:   []byte("hello"),
		Value: []byte("value"),
		Type:  LogRecordNormal,
	}
	res1, n1 := EncodeLogRecord(rec1)
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))
	t.Log(res1)
	t.Log(n1)
	// value为空

	// 对delete	情况的测试
	type args struct {
		logRecord *LogRecord
	}
}

func Test_decodeLogRecordHeader(t *testing.T) {

}

func Test_getLogRecordCRC(t *testing.T) {

}
