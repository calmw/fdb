package fdb

import (
	"errors"
	"fmt"
	"github.com/calmw/fdb/data"
	"github.com/calmw/fdb/fio"
	"github.com/calmw/fdb/index"
	"github.com/calmw/fdb/utils"
	"github.com/gofrs/flock"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB 存储引擎实例
type DB struct {
	options         Options // 配置项
	mu              *sync.RWMutex
	fileIds         []int                     // 文件ID，只能在加载索引的时候使用，不能在其他地方更新和使用
	activeFile      *data.DataFile            // 当前活跃数据文件，可用于写入
	olderFiles      map[uint32]*data.DataFile // 旧的数据文件，只用于读
	index           index.Indexer             // 内存索引
	seqNo           uint64                    // 事务序列号
	isMerging       bool                      // 是否正在merge
	seqNoFileExists bool                      // 存储事务序列号的seqNo文件是否存在
	isInitial       bool                      // 是否初始化数据目录，第一次启动
	fileLock        *flock.Flock              // 文件锁，保证多进程之间（基于同一数据库文件目录的进程）互斥
	bytesWrite      uint                      // 当前累计写了多少字节
	reclaimSize     int64                     // 表示有多少数据是无效的
}

// Stat 存储引擎统计信息
type Stat struct {
	KeyNum      uint  // key的总数量
	DataFileNum uint  // 数据文件的数量
	ReclaimSize int64 // 可以进行merge回收的数据量，字节为单位
	DiskSize    int64 // 数据目录所占磁盘空间大小
}

const (
	seqNoKey   = "seq.no"
	dbFileLock = "db.flock"
)

// Open 打开存储引擎实例
func Open(options Options) (*DB, error) {
	// 对用户输入的配置文件进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	var isInitial bool
	// 判断数据目录是否存在，如果不存在，则创建这个目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err = os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 判断当前目录是否正在使用，单进程使用
	fileLock := flock.New(path.Join(options.DirPath, dbFileLock))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold { // 有其他进程在使用
		return nil, ErrDatabaseIsUsing
	}

	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 { // 目录存在，文件不存在
		isInitial = true
	}

	// 初始化DB实例结构
	db := &DB{
		options: options,
		mu:      &sync.RWMutex{},
		//activeFile: nil,
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrite),
		isInitial:  isInitial,
		fileLock:   fileLock,
	}

	// 加载merge数据目录,将merge后的数据文件和索引文件移动到了数据目录下
	if err = db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// 加载数据文件
	if err = db.loadDataFiles(); err != nil {
		return nil, err
	}

	// B+树索引不需要从数据文件中加载索引
	if db.options.IndexType != IndexTypeBPlusTree {
		// 从hint索引文件加载索引
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}

		// 从数据文件中加载索引
		if err := db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}

		// 重置 IO 类型为标准文件IO
		if options.MMapAtStartup {
			if err = db.resetIOType(); err != nil {
				return nil, err
			}
		}
	}

	// 取出事务序列号,b+树需要从seqNoFile加载seqNo,并从活跃文件获取offset
	if db.options.IndexType == IndexTypeBPlusTree {
		if err = db.loadSeqNo(); err != nil {
			return nil, err
		}
		if db.activeFile != nil {
			size, err := db.activeFile.IOManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOff = size
		}
	}

	return db, nil
}

// Close 关闭数据库
func (db *DB) Close() error {
	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("failed to unlock th dorectory, %v", err))
		}
	}()
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	// 关闭索引，特别是B+树是需要关闭的，毕竟它本是是个数据库实例
	if err := db.index.Close(); err != nil {
		return err
	}

	// 保存当前事务序列号
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record := &data.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
	}
	encRecord, _ := data.EncodeLogRecord(record)
	if err = seqNoFile.Write(encRecord); err != nil {
		return err
	}
	if err = seqNoFile.Sync(); err != nil {
		return err
	}

	// 关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}
	//关闭旧的文件
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Sync 持久化数据文件
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

// Stat 返回数据库的相关统计信息
func (db *DB) Stat() *Stat {
	db.mu.Lock()
	defer db.mu.Unlock()

	dataFiles := uint(len(db.olderFiles))
	dirSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get dir dirSize:%v", err))
	}
	if db.activeFile != nil {
		dataFiles += 1
	}
	return &Stat{
		KeyNum:      uint(db.index.Size()),
		DataFileNum: dataFiles,
		ReclaimSize: db.reclaimSize,
		DiskSize:    dirSize,
	}
}

// Backup 备份数据库，将数据文件拷贝，排除锁文件
func (db *DB) Backup(dir string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return utils.CopyDir(db.options.DirPath, dir, []string{dbFileLock})
}

// Put 写入key/value数据
func (db *DB) Put(key, value []byte) error {
	// 检查key
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加写入到当前文件
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引
	if oldPos := db.index.Put(key, pos); oldPos != nil {
		db.reclaimSize += int64(oldPos.Size) // key之前已经存在，增加无效数据大小
	}

	return nil
}

// Get 根据key读取数据
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	// 检查key
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	// 从内存数据结构中取出key对应的索引信息
	logRecordPos := db.index.Get(key)
	// 如果key不在内存索引中,说明key不存在
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	// 从数据文件中获取value
	return db.getValueByPosition(logRecordPos)
}

// Fold 获取所有的数据，并执行用户指定的操作,函数返回false时终止遍历
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	iterator := db.index.Iterator(false)
	defer iterator.Close()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		val, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), val) {
			break
		}
	}
	return nil
}

// ListKeys 获取数据库中所有的key
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	defer iterator.Close() // B+树的迭代器，读写事务是互斥的，读完，不关闭的话，写不进去，btree和amt其实不需要关闭迭代器

	keys := make([][]byte, db.index.Size())
	idx := 0
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

// 根据索引信息获取对应的value
func (db *DB) getValueByPosition(pos *data.LogRecordPos) ([]byte, error) {
	// 根据文件ID找到数据文件
	var dataFile *data.DataFile
	if db.activeFile.FileId == pos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[pos.Fid]
	}
	// 数据文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}
	// 根据偏移量读取对应的数据
	logRecord, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}

	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return logRecord.Value, nil
}

// Delete 根据key删除数据
func (db *DB) Delete(key []byte) error {
	// 检查key
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// 从内存数据结构中取出key对应的索引信息
	pos := db.index.Get(key)
	// 如果key不在内存索引中,说明key不存在,直接返回
	if pos == nil {
		return nil
	}
	// 构造logRecord，标识其是被删除的
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}
	// 写入数据文件中
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	db.reclaimSize += int64(pos.Size) // 成功删除，增加无效数据大小， 增加删除标识的数据条目大小
	// 从内存索引中，将对应的key删除
	oldPos, ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size) // 成功删除，增加无效数据大小，增加旧数据条目大小
	}

	return nil
}

// 设置当前活跃文件
// 在访问此方法前必须持有互斥锁
func (db *DB) setActiveDataFile() error {
	var initialFiledId uint32 = 0
	if db.activeFile != nil {
		initialFiledId = db.activeFile.FileId + 1
	}
	// 打开新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFiledId, fio.StandardFIO)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// 从磁盘中加载数据文件
func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}
	var fileIds []int
	// 遍历目录中所有文件，找到所有以.data结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			// 数据目录有可能被损坏了
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}
	// 对文件ID进行排序，从小到大依次加载
	sort.Ints(fileIds)
	db.fileIds = fileIds
	// 遍历每个文件ID，打开对应的数据文件
	for i, fid := range fileIds {
		ioType := fio.StandardFIO
		if db.options.MMapAtStartup {
			ioType = fio.MemoryMap
		}
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid), ioType)
		if err != nil {
			return err
		}
		// 最后一个ID最大的说明是当前活跃文件
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else { // 旧的数据文件
			db.olderFiles[uint32(fid)] = dataFile
		}
	}

	return nil
}

// 从数据文件中加载索引,遍历文件中的所有记录，并更新到内存中
func (db *DB) loadIndexFromDataFiles() error {
	// 没有文件，说明数据库是空的
	if len(db.fileIds) == 0 {
		return nil
	}

	// 查看是否发生过merge,如果发生过，加载fid大于nonMergeFileId的即可
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinishedFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinishedFileName); err == nil {
		fId, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fId
	}

	updateIndex := func(key []byte, logType data.LogRecordType, pos *data.LogRecordPos) {
		var oldPos *data.LogRecordPos
		if logType == data.LogRecordDeleted {
			oldPos, _ = db.index.Delete(key)
			db.reclaimSize += int64(pos.Size) // 增加删除标识的数据条目大小
		} else {
			db.index.Put(key, pos)
		}
		if oldPos != nil {
			db.reclaimSize += int64(oldPos.Size) // 增加旧数据条目大小
		}
	}

	// 暂存事务数据,事务ID=>[]数据信息
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo = nonTransactionSeqNo

	// 遍历所有文件ID，处理文件中的记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		// 如果比最近未参与merge的文件id更小。则说明已经从hint文件中加载了索引
		if hasMerge && fileId < nonMergeFileId {
			continue
		}
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		//
		var offset int64
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 构建内存索引并保存
			logRecordPos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: offset,
				Size:   uint32(size),
			}

			// 解析 key 拿到事务序列号
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo { // 非事务操作，直接更新内存索引
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				if logRecord.Type == data.LogRecordTxFinished {
					for _, txRecord := range transactionRecords[seqNo] {
						updateIndex(txRecord.Record.Key, txRecord.Record.Type, txRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else { // 是writeBatch的数据，但还没有到结束标识
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}

			// 更新事务序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			offset += size
		}
		// 如果当前是活跃文件，更新这个文件的writeOff
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}
	// 更新序列号
	db.seqNo = currentSeqNo

	return nil
}

// 追加写数据到活跃文件中
func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.appendLogRecord(logRecord)
}

// 追加写数据到活跃文件中
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {

	// 判断当前活跃数据文件是否存在，因为数据库在没有写入数据的时候是没有文件生成的
	if db.activeFile == nil {
		err := db.setActiveDataFile()
		if err != nil {
			return nil, err
		}
	}

	// 写入数据编码
	encRecord, size := data.EncodeLogRecord(logRecord)
	// 如果写入的数据已经达到了活跃文件阀值，则关闭活跃文件，打开新的文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		// 先持久化数据文件，保证已有的数据持久到磁盘中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		// 将当前活跃文件转换为旧的数据文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile
		// 打开新的活跃文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}
	db.bytesWrite += uint(size)
	// 根据用户配置决定是否持久化
	needSync := db.options.SyncWrite
	// 用户没有设置每次写入持久化,但设置了达到一定字节持久化
	if !needSync && db.options.BytesPerWrite > 0 && db.bytesWrite >= db.options.BytesPerWrite {
		needSync = true
	}
	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		// 清空累计值
		if db.bytesWrite > 0 {
			db.bytesWrite = 0
		}
	}
	// 构造内存索引信息
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
		Size:   uint32(size),
	}
	return pos, nil
}

// 追加写数据到活跃文件中
func (db *DB) loadSeqNo() error {
	fileName := path.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return err
	}
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record, _, err := seqNoFile.ReadLogRecord(0)
	if err != nil {
		return err
	}
	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}
	db.seqNo = seqNo
	db.seqNoFileExists = true

	return os.Remove(fileName) // 防止追加写多条，所以这里删除
}

// 将数据文件的IO类型重置为文件IO
func (db *DB) resetIOType() error {
	if db.activeFile == nil {
		return nil
	}
	// 设置活跃文件IO类型
	if err := db.activeFile.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
		return err
	}
	// 设置旧的数据文件IO类型
	for _, dataFile := range db.olderFiles {
		if err := dataFile.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
			return err
		}
	}

	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}
	if options.DataFileMergeRatio < 0 || options.DataFileMergeRatio > 1 {
		return errors.New("invalid ratio, must between 0 and 1")
	}
	return nil
}
