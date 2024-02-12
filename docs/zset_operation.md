Redis zset 的一些操作命令

bruce.yao

于 2022-01-17 17:59:48 发布

阅读量1.3w
收藏 16

点赞数 6
文章标签： redis zset skiplist 跳表
版权

华为云开发者联盟
该内容已被华为云开发者联盟社区收录
加入社区
Redis zset 的一些使用 - 云+社区 - 腾讯云

Redis基本命令一Sorted Sets操作 - CodeAntenna

Redis Sorted Set有序集合 存储操作方法_SunshineBoySZF_51CTO博客

最近做排行信息的时候用到了 Redis 的 Sorted Set， 写篇文章来和大家分享一波。

Sorted Set (有序集合)

通常我们也称为 zset，指的是在 redis 中，通常以 zset add 等命令操作
zset 通常包含 3 个 关键字操作：

key (与我们 redis 通常操作的 key value 中的key 一致)
score (排序的分数，该分数是有序集合的关键，可以是双精度或者是整数)
member (指我们传入的 obj，与 key value 中的 value 一致)
下面我们来看具体的相关命令

ZADD

ZADD key score member [[score member] [score member] ...]
[[score member] [score member] ...] 在Redis2.4 之后可以添加多个个元素
添加一个或者多个元素到 指定的 key 中。 如果该 key 中已经有了相同的 member，则更新该 member 的 score 值，并重排序；如果 key 存在于 redis 中， 但不是 zset 类型，则返回错误。

示例

# 添加1个元素

redis> ZADD key_1 100 xiaoming
(integer) 1

# 添加多个元素

redis> ZADD key_1 100 xiaoming 20 xiaohong
(integer) 2

#查看元素 score值递增(从小到大)来排序。 如果需要 按score值递减(从大到小)来排列，使用ZREVRANGE命令。
WITHSCORES选项，来让成员和它的score值一并返回

redis> ZRANGE key_1 0 -1 WITHSCORES
1) "xiaohong"
2) "20"
3) "xiaoming"
4) "100"
   可以从上面的例子看出 ZADD 命令还是很容易理解的

ZREM
ZREM key member [member ...]
说完了添加，我们肯定要讲移除了， 与 ZADD 命令一样，也支持多个删除操作（当然也是在2.4版本之后）。这里有个要注意的点，在移除的过程中，如果 member 不存在，将被忽略。

key 存在但是不是 zset，同样会报错

示例

# 移除单个元素

redis> ZREM key_1 xiaohong
(integer) 1


# 移除多个元素

redis> ZREM key_1 xiaohong xiaoming
(integer) 2

# 移除不存在元素

redis> ZREM key_1 xiaolin
(integer) 0
ZCARD
ZCARD key
返回 key 的成员个数。 key不存在时，返回0

# 添加一个 key 及其成员

127.0.0.1:6379> ZADD key_1 100 xiaoming
(integer) 1

# 查找 key 的成员个数

127.0.0.1:6379> ZCARD key_1
(integer) 1

# 添加 第二个成员

127.0.0.1:6379> ZADD key_1 20 xiaohong
(integer) 1

# 可以看到 key_1 的成员个数变成了 2 个

127.0.0.1:6379> ZCARD key_1
(integer) 2

# 查看不存在的 key

127.0.0.1:6379> ZCARD key_2
(integer) 0
ZCOUNT
ZCOUNT key min max
返回指定 key 的 分数在 min 与 max 之间的 member 个数

# 分数在 50 到 100 之间的个数

127.0.0.1:6379> ZCOUNT key_1 50 100
(integer) 1
ZSCORE
ZSCORE key member
返回指定 key 和 member 的 分数值

# 获取 key_1 的成员 xiaoming 的分数值

127.0.0.1:6379> ZSCORE key_1 xiaoming
"100"
ZINCRBY
ZINCRBY key increment member
为指定 key 的 member 的分数值 加 increment，其中 increment 代表数值，increment 可以是 负数，代表减去。

如果 key 或者 member 不存在，代表 ZADD 操作

# 查看 key_1 的 成员 xiaoming  的分数

127.0.0.1:6379> ZSCORE key_1 xiaoming
"100"

# 给 key_1 的 成员 xiaoming 的分数 加上 100
127.0.0.1:6379> ZINCRBY key_1 100 xiaoming
"200"

# 查看 key_1 的 成员 xiaoming  的分数
127.0.0.1:6379> ZSCORE key_1 xiaoming
"200"
ZRANGE
ZRANGE key start stop [WITHSCORES]
返回指定 key 的 指定下标的成员, start stop 代表下标区间。

返回的结果默认按照分数==从小到大==排列，如果需要 ==从大到==小排列，需要是用 ZREVRANGE 命令。

start 和 stop 都以 0 开始，比如，0 为第一个成员，1 为第二个成员。
可以用 -1 表示最后一个成员， -2 表示倒数第二个成员
WITHSCORES 可以返回相关成员 及其分数
# 查看 第一个 和 第二个成员
127.0.0.1:6379> ZRANGE key_1 0 1
1) "xiaohong"
2) "xiaoming"

# 查看所有的成员
127.0.0.1:6379> ZRANGE key_1 0 -1
1) "xiaohong"
2) "xiaoming"

# 查看成员 以及分数

127.0.0.1:6379> ZRANGE key_1 0 -1 WITHSCORES
1) "xiaohong"
2) "20"
3) "xiaoming"
4) "200"
   ZREVRANGE
   ZREVRANGE key start stop [WITHSCORES]
   用法和 ZRANGE 相同，只是排序是按照 分数 从大到小

# 按照分数从大到小排列
127.0.0.1:6379> ZREVRANGE key_1 0 -1 WITHSCORES
1) "xiaoming"
2) "200"
3) "xiaohong"
4) "20"
   ZRANGEBYSCORE
   ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
   返回指定分数的成员。分数在 min max 之间，返回的成员按照 分数 从小到大排列

LIMIT 指定返回结果的区间和 数量，与 sql 中的 limit 一样
# 查看分数在 0 到 200 之间的 成员
127.0.0.1:6379> ZRANGEBYSCORE key_1 0 200
1) "xiaohong"
2) "xiaoming"
   min 和 max 可以带入 开区间和闭区间的概念
# 查看分数在 0 到 199 之间的成员
127.0.0.1:6379> ZRANGEBYSCORE key_1 0 (200
1) "xiaohong"
   ZREVRANGEBYSCORE
   ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
   与ZRANGEBYSCORE用法一样，只是返回的成员按照 分数从大到小排列

# 注意 分数要按照 max min 写，否则结果是空值
127.0.0.1:6379> ZREVRANGEBYSCORE key_1 200 0
1) "xiaoming"
2) "xiaohong

# 错误示例
127.0.0.1:6379> ZREVRANGEBYSCORE key_1 0 200
(empty list or set)
ZRANK
ZRANK key member
返回指定key 的成员排名，按照分数 从小到大排列，其中==返回的排名是以 0 开始==

# 查看 key_1 的成员
127.0.0.1:6379> ZRANGE key_1 0 -1
1) "xiaohong"
2) "xiaoming"

# 查看xiaoming 的排名
127.0.0.1:6379> ZRANK key_1 xiaoming
(integer) 1

# 查看xiaohong 的排名
127.0.0.1:6379> ZRANK key_1 xiaohong
(integer) 0
ZREVRANK
ZREVRANK key member
与 ZRANK 的用法相同，区别就是按照分数 从大到小排列

127.0.0.1:6379> ZRANGE key_1 0 -1
1) "xiaohong"
2) "xiaoming"

# 反转排序
127.0.0.1:6379> ZREVRANK key_1 xiaohong
(integer) 1
ZREMRANGEBYRANK
ZREMRANGEBYRANK key start stop
移除指定 key 的指定排名介于 start 和 stop 之间的成员，同样排名以 0 开始。

# 查看排名
127.0.0.1:6379> ZRANGE key_1 0 -1
1) "xiaohong"
2) "xiaoming"

# 移除 排名 为0 的成员
127.0.0.1:6379> ZREMRANGEBYRANK key_1 0 0
(integer) 1

# 查看排名，已移除
127.0.0.1:6379> ZRANGE key_1 0 -1
1) "xiaoming"
   ZREMRANGEBYSCORE
   ZREMRANGEBYSCORE key min max
   移除指定key的 分数介于 min 和 max 之间的成员

# 查看成员
127.0.0.1:6379> ZRANGE key_1 0 -1 WITHSCORES
1) "xiaohong"
2) "20"
3) "xiaoming"
4) "200"

# 移除分数为 0 到 100 之间的成员
127.0.0.1:6379> ZREMRANGEBYSCORE key_1 0 100
(integer) 1

# 查看成员
127.0.0.1:6379> ZRANGE key_1 0 -1 WITHSCORES
1) "xiaoming"
2) "200"
   ZINTERSTORE
   ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
   上面的命令这么多，其实讲明了就是下面这几个

destination 给定的一个新的集合
numkeys 计算的几个集合
之后的就是集合到的key了 举例
# 查看 key_1 的成员及其 分数
127.0.0.1:6379> ZRANGE key_1 0 -1 WITHSCORES
1) "xiaohong"
2) "50"
3) "xiaoming"
4) "200"

# 查看 key_2 的成员及其分数
127.0.0.1:6379> ZRANGE key_2 0 -1 WITHSCORES
1) "xiaohong"
2) "70"
3) "xiaoming"
4) "100"

# 把 key_1 和 key_2 根据 成员 把相关的 分数加起来到一个新的集合 sum_key
127.0.0.1:6379> ZINTERSTORE sum_key 2 key_1 key_2
(integer) 2
127.0.0.1:6379> ZRANGE sum_key 0 -1 WITHSCORES
1) "xiaohong"
2) "120"
3) "xiaoming"
4) "300"
   ZUNIONSTORE
   ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
   这个命令和上面的命令用法基本相同 只是有个算法因子的参数,比如 key_1 key_2 WEIGHTS 2 2 那么key_1 的分数和key_2 的分数 各自乘以 2

ZUNIONSTORE 用于计算给定的一个或多个有序集的==并集==。 ZINTERSTORE 则用于计算给定的一个或多个有序集的==交集==。

# 查看 key_2 的成员及其分数
127.0.0.1:6379> ZRANGE key_2 0 -1 WITHSCORES
1) "xiaohong"
2) "70"
3) "xiaoming"
4) "100"

# 查看 key_3 的成员及其分数
127.0.0.1:6379> ZRANGE key_3 0 -1 WITHSCORES
1) "xiaoli"
2) "80"
3) "xiaoxia"
4) "180"

# UNION key_2 key_3　没有写　乘法因子　默认是　１
127.0.0.1:6379> ZUNIONSTORE union0 2 key_2 key_3
(integer) 4
127.0.0.1:6379> ZRANGE union0 0 -1 WITHSCORES
1) "xiaohong"
2) "70"
3) "xiaoli"
4) "80"
5) "xiaoming"
6) "100"
7) "xiaoxia"
8) "180"

# UNION key_2 key_3　key_2 和 key_3 各自乘以2
127.0.0.1:6379> ZUNIONSTORE union1 2 key_2 key_3 WEIGHTS 2 2
(integer) 4
127.0.0.1:6379> ZRANGE union1 0 -1 WITHSCORES
1) "xiaohong"
2) "140"
3) "xiaoli"
4) "160"
5) "xiaoming"
6) "200"
7) "xiaoxia"
8) "360"