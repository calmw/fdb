package fdb

import (
	"github.com/calmw/fdb/data"
	"github.com/calmw/fdb/utils"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	mergeDirName     = "-merge"
	mergeFinishedKey = "merge.finished"
)

// Merge 清理无效数据文件
func (db *DB) Merge() error {
	if db.activeFile == nil { // 如果数据库为空，则直接返回
		return nil
	}
	db.mu.Lock()
	if db.isMerging { // 如果正在进行当中，则直接返回
		db.mu.Unlock()
		return ErrMergeIsProgress
	}

	// 检查是否达到了可以merge的阀值
	totalSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		db.mu.Unlock()
		return err
	}
	if float32(db.reclaimSize)/float32(totalSize) < db.options.DataFileMergeRatio {
		db.mu.Unlock()
		return ErrMergeRatioUnreached
	}
	// 查看剩余空间是否可以容纳merge后的数据量
	availableDiskSize, err := utils.AvailableDiskSize()
	if err != nil {
		db.mu.Unlock()
		return err
	}
	if uint64(totalSize-db.reclaimSize) >= availableDiskSize {
		db.mu.Unlock()
		return ErrNotEnoughSpaceForMerge
	}

	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	// 持久化当前活跃文件
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}
	// 将当前活跃文件，转化为旧的数据文件
	db.olderFiles[db.activeFile.FileId] = db.activeFile
	// 打开新的活跃文件
	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return nil
	}

	// 记录最近没有参与merge的文件id
	nonMergFileId := db.activeFile.FileId

	// 取出所有需要merge的文件
	var mergeFiles []*data.DataFile

	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	db.mu.Unlock()

	// 对待merge的文件从小打到进行排序，依次merge
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	mergePath := db.getMergePath()
	// 如果目录存在，说明发生过merge，将其删掉
	if _, err := os.Stat(mergePath); err != nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	// 新建一个merge path 的目录
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}
	// 打开一个新的db实例
	mergeOptions := db.options
	mergeOptions.DirPath = mergePath
	mergeOptions.SyncWrite = false
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}
	// 打开hint文件存储索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}
	// 遍历处理每个数据文件
	for _, dataFile := range mergeFiles {
		var offset int64
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 解析拿到实际的key
			realKey, _ := parseLogRecordKey(logRecord.Key)
			logRecordPos := db.index.Get(realKey)
			// 和内存索引位置进行比较，如果有效则重写
			if logRecordPos != nil && logRecordPos.Fid == dataFile.FileId && logRecordPos.Offset == offset {
				// 清除事务标记
				logRecord.Key = logRecordKeyWithSeq(realKey, nonTransactionSeqNo)
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}
				// 将位置索引写到hint文件里面，格式和正常数据格式一样，key存储realKey,value存储pos
				err = hintFile.WriteHintRecord(realKey, pos)
				if err != nil {
					return err
				}
			}

			// 增加offset
			offset += size
		}
	}
	// sync 保证持久化
	if err = hintFile.Sync(); err != nil {
		return err
	}
	if err = mergeDB.Sync(); err != nil {
		return err
	}

	// 写标识merge完成的文件
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	mergeFinishedRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergFileId))),
		//Type:  0, // 默认值0 普通类型
	}

	encRecord, _ := data.EncodeLogRecord(mergeFinishedRecord)
	if err = mergeFinishedFile.Write(encRecord); err != nil {
		return err
	}
	if err = mergeFinishedFile.Sync(); err != nil {
		return err
	}

	return nil
}

func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.options.DirPath))
	base := path.Base(db.options.DirPath)
	return path.Join(dir, base+mergeDirName)
}

// 加载merge数据目录
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	// merge 目录不存在则直接返回
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()
	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}
	// 查找标识merge完成的文件，判断merge是否处理完了
	var mergeFinished bool
	var mergeFileNames []string
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		}
		// B+树用到的事务序列号文件，不需要移动，在关闭数据库时候会更新
		if entry.Name() == data.SeqNoFileName {
			continue
		}
		if entry.Name() == dbFileLock { // 数据库锁文件所不复制
			continue
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	// 没有merge完成，直接返回
	if !mergeFinished {
		return nil
	}

	//
	nonMergedFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return nil
	}

	// 删除旧的数据文件,删除小于nonMergedFileId的文件
	var fileId uint32
	for ; fileId < nonMergedFileId; fileId++ {
		fileName := data.GetDataFileName(db.options.DirPath, fileId)
		if _, err = os.Stat(fileName); err == nil { // 旧数据文件存在
			if err = os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	// 将新的数据文件移动过来
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		destPath := filepath.Join(db.options.DirPath, fileName)
		if err = os.Rename(srcPath, destPath); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}
	record, _, err := mergeFinishedFile.ReadLogRecord(0) // 只有一条数据，所以offset是0
	if err != nil {
		return 0, err
	}
	nonMergedFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}

	return uint32(nonMergedFileId), nil
}

// 从hint文件中加载索引
func (db *DB) loadIndexFromHintFile() error {
	// 查看hint索引文件是否存在
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}
	// 打开hint索引文件
	hintFile, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}
	// 读取文件中的索引
	var offset int64
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		logRecordPos := data.DecodeLogRecordPos(logRecord.Value)
		db.index.Put(logRecord.Key, logRecordPos)
		offset += size
	}

	return nil
}
