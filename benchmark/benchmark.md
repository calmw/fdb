#### 基准测试
```shell
cd benchmark
go test -bench=.                      # 测试时间1s
go test -bench=. -benchtime=5s        # 测试时间5s
go test -bench=. -benchtime=1000000x  # 测试1000000次
```
#### 测试结果
![img.png](bench.png)