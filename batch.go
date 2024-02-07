package fdb

import (
	"encoding/binary"
	"fdb/data"
	"sync"
	"sync/atomic"
)

const nonTransactionSeqNo uint64 = 0 // 非事务的seqNo
var txFinKey = []byte("tx-fin")      // 事务完成标识记录的key

// WriteBatch 原子批量写数据
type WriteBatch struct {
	options       WriteBatchOptions
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord // 暂存用户写入数据
}

func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	// B+树索引，不是第一次加载，并且序列号文件不存，禁用writeBatch （原因可能是上一次未正常关闭数据库，导致序列号文件未写成功）
	if db.options.IndexType == IndexTypeBPlusTree && !db.seqNoFileExists && !db.isInitial {
		panic("can not use write batch, seqNo file dose not exists")
	}
	return &WriteBatch{
		options:       opts,
		mu:            &sync.Mutex{},
		db:            db,
		pendingWrites: map[string]*data.LogRecord{},
	}
}

func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()
	// 暂存logRecord
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}
	wb.pendingWrites[string(key)] = logRecord

	return nil
}

// Delete 删除数据
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 数据不存在则直接删除
	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	//暂存logRecord
	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}
	wb.pendingWrites[string(key)] = logRecord

	return nil
}

// Commit 提交事物，将暂存的数据写到数据文件，并更新内存索引
func (wb *WriteBatch) Commit() error {

	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}
	if len(wb.pendingWrites) > wb.options.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}

	// 加锁，保证当前事务提交串行话
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	// 获取当前最新的事务序列号
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	// 开始写数据到文件当中
	positions := make(map[string]*data.LogRecordPos)
	for _, record := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeq(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		positions[string(record.Key)] = logRecordPos
	}

	// 写一条标识事务完成的数据
	finishRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(txFinKey, seqNo),
		Type: data.LogRecordTxFinished,
	}
	if _, err := wb.db.appendLogRecord(finishRecord); err != nil {
		return err
	}

	// 根据配置持久化数据
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 更新内存索引
	for _, record := range wb.pendingWrites {
		pos := positions[string(record.Key)]
		if record.Type == data.LogRecordNormal {
			wb.db.index.Put(record.Key, pos)
		} else if record.Type == data.LogRecordDeleted {
			wb.db.index.Delete(record.Key)
		}
	}

	// 清空暂存数据
	wb.pendingWrites = map[string]*data.LogRecord{}

	return nil
}

// key + sewNo 编码
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	enKey := make([]byte, n+len(key))
	copy(enKey[:n], seq[:n])
	copy(enKey[n:], key)

	return enKey
}

// 解析logRecord的key,获取实际的key和事务序列号
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
