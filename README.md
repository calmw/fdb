## FDB是什么？

- FDB 是一个轻量、快速、可靠的 KV 存储引擎。

## 主要特点

### 优势

<details>
    <summary><b>读写低延迟</b></summary>
    这是由于存储模型文件的追加写入特性，充分利用顺序 IO 的优势。
</details>

<details>
    <summary><b>高吞吐量，即使数据完全无序</b></summary>
    写入 FDB 的数据不需要在磁盘上排序，fdb 的日志结构文件设计在写入过程中减少了磁盘磁头的移动。
</details>

<details>
    <summary><b>能够处理大于内存的数据集，性能稳定</b></summary>
    FDB 的数据访问涉及对内存中的索引数据结构进行直接查找，这使得即使数据集非常大，查找数据也非常高效。
</details>

<details>
    <summary><b>一次磁盘 IO 可以获取任意键值对</b></summary>
    FDB 的内存索引数据结构直接指向数据所在的磁盘位置，不需要多次磁盘寻址来读取一个值，有时甚至不需要寻址，这归功于操作系统的文件系统缓存以及 WAL 的 block 缓存。
</details>

<details>
    <summary><b>性能快速稳定</b></summary>
    FDB 写入操作最多需要一次对当前打开文件的尾部的寻址，然后进行追加写入，写入后会更新内存。这个流程不会受到数据库数据量大小的影响，因此性能稳定。
</details>

<details>
    <summary><b>崩溃恢复快速</b></summary>
    使用 FDB 的崩溃恢复很容易也很快，因为 FDB 文件是只追加写入一次的。恢复操作需要检查记录并验证CRC数据，以确保数据一致。
</details>

<details>
    <summary><b>备份简单</b></summary>
    在大多数系统中，备份可能非常复杂。FDB 通过其只追加写入一次的磁盘格式简化了此过程。任何按磁盘块顺序存档或复制文件的工具都将正确备份或复制 FDB 数据库。
</details>

<details>
    <summary><b>批处理操作可以保证原子性、一致性和持久性</b></summary>
    FDB 支持批处理操作，这些操作是原子、一致和持久的。批处理中的新写入操作在提交之前被缓存在内存中。如果批处理成功提交，批处理中的所有写入操作将持久保存到磁盘。如果批处理失败，批处理中的所有写入操作将被丢弃。
    即一个批处理操作中的所有写入操作要么全部成功，要么全部失败。
</details>

<details>
    <summary><b>支持可以反向和正向迭代的迭代器</b></summary>
    FDB 支持正向和反向迭代器，这些迭代器可以在数据库中的任何位置开始迭代。迭代器可以用于扫描数据库中的所有键值对，也可以用于扫描数据库中的某个范围的键值对，迭代器从索引中获取位置信息，然后直接从磁盘中读取数据，因此迭代器的性能非常高。
</details>

<details>
    <summary><b>支持 Key 的过期时间</b></summary>
    FDB 支持为 key 设置过期时间，过期后 key 将被自动删除。
</details>

<details>
    <summary><b>兼容Redis协议</b></summary>
    当前支持的方法有：set、setex、get、hset、hget、hdel、sadd、sismember、srem、lpush、rpush、lpop、rpop、zadd、zscore
</details>

<details>
    <summary><b>支持HTTP服务</b></summary>
    当前支持的方法有：put、get、delete、listkeys、stat
</details>