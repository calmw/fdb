package utils

import (
	"fmt"
	"github.com/shirou/gopsutil/disk"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// DirSize 获取一个目录的大小,单位字节
func DirSize(dirPath string) (int64, error) {
	var size int64
	// 对目录进行遍历操作
	err := filepath.Walk(dirPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// AvailableDiskSize 获取当前磁盘(非目录)剩余可用空间大小,单位字节
func AvailableDiskSize() (uint64, error) {
	wd, err := syscall.Getwd()
	if err != nil {
		return 0, err
	}
	usages, err := disk.Usage(wd) // 获取根目录磁盘使用情况
	if err != nil {
		fmt.Println(err)
		return 0, err
	}
	//fmt.Printf("Total: %v, Free: %v, Used: %v, UsedPercent: %v%%\n", usages.Total, usages.Free, usages.Used, usages.UsedPercent)
	return usages.Free, nil
}

// CopyDir 拷贝数据目录,排除exclude
func CopyDir(src, dest string, exclude []string) error {
	// 目标目标不存在则创建
	if _, err := os.Stat(dest); os.IsNotExist(err) {
		if err = os.MkdirAll(dest, os.ModePerm); err != nil {
			return err
		}
	}

	return filepath.Walk(src, func(path string, info fs.FileInfo, err error) error {
		fileName := strings.Replace(path, src, "", 1)
		if fileName == "" {
			return nil
		}

		for _, e := range exclude {
			matched, err := filepath.Match(e, info.Name())
			if err != nil {
				return err
			}
			if matched {
				return nil
			}
		}

		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dest, fileName), info.Mode())
		}

		data, err := os.ReadFile(filepath.Join(src, fileName))
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dest, fileName), data, info.Mode())
	})
}
