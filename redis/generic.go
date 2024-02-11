package redis

import "errors"

/// 对几种数据类型都适用

// Del 删除
func (rds *RedisDataStructure) Del(key []byte) error {
	return rds.db.Delete(key)
}

// Type 获取类型
func (rds *RedisDataStructure) Type(key []byte) (redisDataType, error) {
	// 调用存储引擎接口读取数据
	encValue, err := rds.db.Get(key)
	if err != nil {
		return 0, err
	}
	if len(encValue) == 0 {
		return 0, errors.New("value is null")
	}

	// 第一个字节是类型
	return redisDataType(encValue[0]), nil
}
