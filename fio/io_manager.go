package fio

const DataFilePerm = 0644

type FileIOType byte

const (
	StandardFIO FileIOType = iota // 标准文件IO
	MemoryMap                     // 内存文件映射
)

// IOManager 抽象IO管理接口，可以介入不同的IO类型，目前支持标准文件IO
type IOManager interface {
	Read([]byte, int64) (int, error) // 从文件的给定位置读取对应的数据
	Write([]byte) (int, error)       // 写入字节数组到文件中
	Sync() error                     // 持久化数据
	Close() error                    // 关闭文件
	Size() (int64, error)            // 获取到文件的大小
}

// NewIOManager 初始化IOManager
func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardFIO:
		return NewFileIOManager(fileName)
	case MemoryMap:
		return NewMMapIOManager(fileName)
	default:
		panic("unsupported io type")
	}

}
