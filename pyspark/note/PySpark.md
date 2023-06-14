# pyspark

## 1. RDD
### 1.1 为什么需要RDD
- 分区控制
- shuffle控制
- 数据存储、序列化、发送
- 数据计算API
- 等一系列功能

在分布式框架中，需要有一个统一的数据抽象对象，来实现上述分布式计算所需功能，这个对象就是RDD。

### 1.2 什么是RDD
    RDD(Resilient Distributed Dataset)弹性分布式数据集，是Spark中最基本的数据抽象，是不可变的分布式对象集合，每个RDD都被分为多个分区，每个分区就是一个数据集片段，RDD可以跨集群节点进行计算。
- Dataset：数据集合,用来存放数据的（本地集合数据全部在一个进程内部）
- Distributed：RDD中的数据是分布式存储的,可用于分布式计算（RDD的数据是跨越机器存储的）
- Resilient：RDD中的数据是弹性的,可以动态扩容，存储在内存或者磁盘中
- RDD是一个只读的数据集合，不能修改，只能进行转换操作，转换操作会产生新的RDD

### 1.3 RDD的五大特性
- A list of partitions 分区列表：RDD中的数据被分为多个分区，每个分区就是一个数据集片段（RDD是有分区的）
  - 分区是RDD数据存储的最小单位
```python
rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10], 3)
rdd.glom().collect()
# [[1, 2, 3], [4, 5, 6], [7, 8, 9, 10]]
```
- A function for computing each split 一个函数用来计算每个分区：RDD中的数据是通过函数计算得到的（计算方法都会作用到每一个分区之上）
```python
rdd = sc.paralellize([1,2,3,4,5,6,7,8,9,10], 3).glom().collect()
# [[1, 2, 3], [4, 5, 6], [7, 8, 9, 10]]
rdd = sc.paralellize([1,2,3,4,5,6,7,8,9,10], 3).map(lambda x: x+1).glom().collect()
# [[2, 3, 4], [5, 6, 7], [8, 9, 10, 11]]
```
- A list of dependencies on other RDDs RDD的依赖列表：RDD中的数据是通过其他RDD转换得到的(RDD之间有相互依赖的关系,迭代计算)
```python
sc = SparkContext(conf = conf)
rdd1 = sc.textFile("file:///home/hadoop/data/word.txt")
rdd2 = rdd1.flatMap(lambda x: x.split(" "))
rdd3 = rdd2.map(lambda x: (x, 1))
rdd4 = rdd3.reduceByKey(lambda x, y: x+y)
rdd4.collect()
# [('hello', 2), ('world', 2), ('spark', 1), ('hadoop', 1)]
# textFile - > flatMap - > map - > reduceByKey
```
- Optionally, a Partitioner for key-value RDDs (e.g. to say that the RDD is hash-partitioned) 可选的，一个分区器用来对键值对RDD进行分区（KV型RDD可以有分区器）
  - RDD内存储的数据是二元元组，第一个元素是key，第二个元素是value。例如：("pyspark", 3)
  - Hash分区规则 ("hadoop", 3)("hadoop", 3)("flik", 1)("pyspark", 3)("pyspark", 3)
  - rdd.partitionBy 方法来设置
  - 不是所有的RDD都是KV型的，只有KV型的RDD才有分区器
- Optionally, a list of preferred locations to compute each split on (e.g. block locations for an HDFS file) 可选的，一个列表用来指定每个分区的优先计算位置（RDD分区数据的读取会尽量靠近数据所在地）
  - 在初始RDD（读取数据的时候）规划的时候，分区会尽量规划到存储数据所在的服务器上，这样就可以直接读取硬盘数据，避免网络读取
  - 本地读取：Excutor所在服务器，同样是一个DataNode，同时在DataNode上有它要读的数据，所以可以直接读取机器硬盘即可，无需走网络传输
  - 网络读取：Excutor所在服务器，不是一个DataNode，所以要读取数据，必须走网络传输
  - 总结：Spark会在`确保并行计算能力的前提下`，尽量确保本地读取，尽量确保，而不是100%确保，所以这个特性也是`可能的`
### 1.4 WordCount案例分析
```python
# coding:utf8
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("WordCountHelloWorld")
    # 通过SparkConf对象构建SparkContext对象
    sc = SparkContext(conf=conf)

    # 需求 : wordcount单词计数, 读取HDFS上的words.txt文件, 对其内部的单词统计出现 的数量
    # 读取文件
    file_rdd = sc.textFile("hdfs://node1:8020/input/words.txt")

    # 将单词进行切割, 得到一个存储全部单词的集合对象
    words_rdd = file_rdd.flatMap(lambda line: line.split(" "))

    # 将单词转换为元组对象, key是单词, value是数字1
    words_with_one_rdd = words_rdd.map(lambda x: (x, 1))

    # 将元组的value 按照key来分组, 对所有的value执行聚合操作(相加)
    result_rdd = words_with_one_rdd.reduceByKey(lambda a, b: a + b)

    # 通过collect方法收集RDD的数据打印输出结果
    print(result_rdd.collect())

```
![](.PySpark_images/WordCount代码执行的图示.png)

## 2. RDD核心变成
### 2.1 程序执行入口SparkContext对象
* Spark RDD 编程的程序入口对象是SparkContext对象(不论何种编程语言)
* 只有构建出SparkContext, 基于它才能执行后续的API调用和计算
* 本质上, SparkContext对编程来说, 主要功能就是创建第一个RDD出来
![](.PySpark_images/432d108d.png)

### 2.2 RDD创建
* 通过并行化集合创建（本地对象转分布式RDD）
  * 通过SparkContext对象的parallelize方法
```python
# coding:utf8

# 导入Spark的相关包
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # 0. 初始化执行环境 构建SparkContext对象
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 演示通过并行化集合的方式去创建RDD, 本地集合 -> 分布式对象(RDD)
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9])
    # parallelize方法, 没有给定 分区数, 默认分区数是多少?  根据CPU核心来定
    print("默认分区数: ", rdd.getNumPartitions())

    rdd = sc.parallelize([1, 2, 3], 3)
    print("分区数: ", rdd.getNumPartitions())

    # collect方法, 是将RDD(分布式对象)中每个分区的数据, 都发送到Driver中, 形成一个Python List对象
    # collect: 分布式 转 -> 本地集合
    print("rdd的内容是: ", rdd.collect())
```
* 通过外部数据集创建RDD（读取文件）
  * **sparkcontext.textFile(参数1,参数2)**
  * `参数1`：读取文件的路径（必选）
  * `参数2`：最小分区数（可选）
  * 注意：最小分区数是参考值，Spark有自己的判断，你给的太大Spark不会理会
```python
# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # 构建SparkContext对象
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 通过textFile API 读取数据

    # 读取本地文件数据
    file_rdd1 = sc.textFile("../data/input/words.txt")
    print("默认读取分区数: ", file_rdd1.getNumPartitions())
    print("file_rdd1 内容:", file_rdd1.collect())

    # 加最小分区数参数的测试
    file_rdd2 = sc.textFile("../data/input/words.txt", 3)
    # 最小分区数是参考值, Spark有自己的判断, 你给的太大Spark不会理会
    file_rdd3 = sc.textFile("../data/input/words.txt", 100)
    print("file_rdd2 分区数:", file_rdd2.getNumPartitions())
    print("file_rdd3 分区数:", file_rdd3.getNumPartitions())

    # 读取HDFS文件数据测试
    hdfs_rdd = sc.textFile("hdfs://node1:8020/input/words.txt")
    print("hdfs_rdd 内容:", hdfs_rdd.collect())
```
  * **sc.wholeTextFiles(参数1)**
  * `参数1`：读取文件夹的路径（必选）
```python
# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 读取小文件文件夹
    rdd= sc.wholeTextFiles("../data/input/tiny_files")
    print(rdd.map(lambda x:x[1]).collect())

```
### 2.3 RDD算子
#### 2.3.1 RDD算子是什么
* 分布式集合对象上的API称之为算子

#### 2.3.2 RDD算子的分类
* Transformation转换算子
  * 定义：RDD的算子，返回值仍然是RDD，但是不会触发作业的执行
  * 特性：这类算子是lazy懒加载的，如果没有action算子的话，Transformation算子是不会执行的
* Action行动算子
  * 定义：RDD的算子，返回值不再是RDD，而是其他的数据类型，会触发作业的执行

### 2.4 常用Transformation算子
#### 2.4.1 map算子
* 功能：map算子，是将RDD的数据一条一条处理（处理的逻辑基于map算子中接受的处理函数），返回新的RDD
* 语法：`rdd.map(func)`
* 参数：func是一个函数，接受RDD中的每一条数据，返回值是处理后的数据
```python
# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6], 3)

    # 定义方法, 作为算子的传入函数体
    def add(data):
        return data * 10


    print(rdd.map(add).collect())

    # 更简单的方式 是定义lambda表达式来写匿名函数
    print(rdd.map(lambda data: data * 10).collect())
    # 输出结果: [10, 20, 30, 40, 50, 60]
   
"""
对于算子的接受函数来说, 两种方法都可以
lambda表达式 适用于 一行代码就搞定的函数体, 如果是多行, 需要定义独立的方法.
"""
```
#### 2.4.2 flatMap算子
* 功能：对rdd执行map操作，然后进行`解除嵌套`操作（多合一）
```python
# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(["hadoop spark hadoop", "spark hadoop hadoop", "hadoop flink spark"])
    # 得到所有的单词, 组成RDD, flatMap的传入参数 和map一致, 就是给map逻辑用的, 解除嵌套无需逻辑(传参)
    rdd2 = rdd.flatMap(lambda line: line.split(" "))
    print(rdd2.collect())
    # 输出结果: ['hadoop', 'spark', 'hadoop', 'spark', 'hadoop', 'flink', 'spark']
```
#### 2.4.3 reduceByKey算子
* 功能：`针对kv型`RDD，自动按照key分组，然后根据你提供的聚合逻辑，完成`组内数据（value）`的聚合操作
* 语法：`rdd.reduceByKey(func)`
* 参数：func是一个函数，接受2个传入参数（类型要一致），返回一个返回值，类型和传入要求一致
```python
# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('a', 1), ('b', 1), ('b', 1), ('a', 1)])

    # reduceByKey 对相同key 的数据执行聚合相加
    print(rdd.reduceByKey(lambda a, b: a + b).collect())
    # 输出结果: [('a', 3), ('b', 2)]
```
#### 2.4.4 mapValues算子
* 功能：针对`二元元组RDD`，对其内部的二元元组的`Value`执行`map`操作
* 语法：`rdd.mapValues(func)`
* 参数：func是一个函数，注意：传入的参数，是二元元组的value值，返回值是处理后的value值
```python
rdd = sc.parllelize([('a', 1), ('a', 11), ('b', 3), ('b', 5)])
print(rdd.mapValues(lambda x: x + 1).collect())
# 输出结果: [('a', 2), ('a', 12), ('b', 4), ('b', 6)]
```
#### WrodCount案例
* 功能：统计文件中每个单词出现的次数
```python
# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 1. 读取文件获取数据 构建RDD
    file_rdd = sc.textFile("../data/input/words.txt")

    # 2. 通过flatMap API 取出所有的单词
    word_rdd = file_rdd.flatMap(lambda x: x.split(" "))

    # 3. 将单词转换成元组, key是单词, value是1
    word_with_one_rdd = word_rdd.map(lambda word: (word, 1))

    # 4. 用reduceByKey 对单词进行分组并进行value的聚合
    result_rdd = word_with_one_rdd.reduceByKey(lambda a, b: a + b)

    # 5. 通过collect算子, 将rdd的数据收集到Driver中, 打印输出
    print(result_rdd.collect())
    
    # 输出结果: [('hello', 2), ('spark', 2), ('hadoop', 1), ('flink', 1)]
```
#### 2.4.5 groupBy算子
```python
# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('a', 1), ('b', 1), ('b', 2), ('b', 3)])

    # 通过groupBy对数据进行分组
    # groupBy传入的函数的 意思是: 通过这个函数, 确定按照谁来分组(返回谁即可)
    # 分组规则 和SQL是一致的, 也就是相同的在一个组(Hash分组)
    result = rdd.groupBy(lambda t: t[0])
    print(result.map(lambda t:(t[0], list(t[1]))).collect())
    # 输出结果: [('a', [('a', 1), ('a', 1)]), ('b', [('b', 1), ('b', 2), ('b', 3)])]
```
#### 2.4.6 filter算子
* 功能：对RDD中的每个元素进行过滤操作，符合的保留，返回一个新的RDD
```python
# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6])

    # 通过Filter算子, 过滤奇数
    result = rdd.filter(lambda x: x % 2 == 1)

    print(result.collect())
    # 输出结果: [1, 3, 5]
```
#### 2.4.7 distinct算子
* 功能：对RDD中的元素进行去重操作，返回一个新的RDD
```python
# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 1, 1, 2, 2, 2, 3, 3, 3])

    # distinct 进行RDD数据去重操作
    print(rdd.distinct().collect())
    # 输出结果: [1, 2, 3]

    rdd2 = sc.parallelize([('a', 1), ('a', 1), ('a', 3)])
    print(rdd2.distinct().collect())
    # 输出结果: [('a', 1), ('a', 3)]
```
#### 2.4.8 union算子
* 功能：对两个RDD进行合并操作，返回一个新的RDD
```python
# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([1, 1, 3, 3])
    rdd2 = sc.parallelize(["a", "b", "a"])

    rdd3 = rdd1.union(rdd2)
    print(rdd3.collect())
    # 输出结果: [1, 1, 3, 3, 'a', 'b', 'a']

"""
1. 可以看到 union算子是不会去重的
2. RDD的类型不同也是可以合并的.
"""
```
#### 2.4.9 join算子
* 功能：对两个RDD进行内\外连接操作，返回一个新的RDD
* 注意：join算子只能用于二元元组，并且两个RDD的key类型必须一致
```python
# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([ (1001, "zhangsan"), (1002, "lisi"), (1003, "wangwu"), (1004, "zhaoliu") ])
    rdd2 = sc.parallelize([ (1001, "销售部"), (1002, "科技部")])

    # 通过join算子来进行rdd之间的关联
    # 对于join算子来说 关联条件 按照二元元组的key来进行关联
    print(rdd1.join(rdd2).collect())
    # 输出结果: [(1001, ('zhangsan', '销售部')), (1002, ('lisi', '科技部'))]
    
    # 左外连接, 右外连接 可以更换一下rdd的顺序 或者调用rightOuterJoin即可
    print(rdd1.leftOuterJoin(rdd2).collect())
    # 输出结果: [(1001, ('zhangsan', '销售部')), (1002, ('lisi', '科技部')), (1003, ('wangwu', None)), (1004, ('zhaoliu', None))]
```
#### 2.4.10 intersection算子
* 功能：对两个RDD进行交集操作，返回一个新的RDD
```python
# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([('a', 1), ('a', 3)])
    rdd2 = sc.parallelize([('a', 1), ('b', 3)])

    # 通过intersection算子求RDD之间的交集, 将交集取出 返回新RDD
    rdd3 = rdd1.intersection(rdd2)

    print(rdd3.collect())
    # 输出结果: [('a', 1)]
```
#### 2.4.11 glom算子
* 功能：将RDD的数据加上嵌套，这个嵌套按照分区进行划分，返回一个新的RDD
* 注意：glom算子只能用于RDD，不能用于键值对RDD
```python
# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 2)

    print(rdd.glom().flatMap(lambda x: x).collect())
    # 输出结果: [[1, 2, 3, 4], [5, 6, 7, 8, 9]]
```
#### 2.4.12 groupByKey算子
* 功能：对键值对RDD中的`key`进行分组，返回一个新的RDD
* 注意：groupByKey算子只能用于`键值对RDD`
* 注意：groupByKey算子会导致数据倾斜，不建议使用
* 没有聚合 仅仅是分组。reduceByKey算子是分组+聚合
```python
# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('a', 1), ('b', 1), ('b', 1), ('b', 1)])

    rdd2 = rdd.groupByKey()

    print(rdd2.map(lambda x: (x[0], list(x[1]))).collect())
    # 输出结果: [('a', [1, 1]), ('b', [1, 1, 1])]
```
#### 2.4.13 soryBy算子
* 功能：对RDD数据进行排序，基于你指定的排序依据
* 语法：sortBy(func, ascending=True, numPartitions=None)
* 参数1：func表示排序依据，可以是数字，也可以是自定义的规则
* 参数2：ascending=True表示升序，False表示降序
* 参数3：numPartitions=None表示排序后的RDD的分区数，如果不指定，默认和排序前的RDD的分区数一致
```python
# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('c', 3), ('f', 1), ('b', 11), ('c', 3), ('a', 1), ('c', 5), ('e', 1), ('n', 9), ('a', 1)], 3)

    # 使用sortBy对rdd执行排序

    # 按照value 数字进行排序
    # 参数1函数, 表示的是 ,  告知Spark 按照数据的哪个列进行排序
    # 参数2: True表示升序 False表示降序
    # 参数3: 排序的分区数
    """注意: 如果要全局有序, 排序分区数请设置为1"""
    print(rdd.sortBy(lambda x: x[1], ascending=True, numPartitions=1).collect())
    # 输出结果: [('f', 1), ('a', 1), ('e', 1), ('c', 3), ('c', 3), ('c', 5), ('n', 9), ('b', 11)]
    
    # 按照key来进行排序
    print(rdd.sortBy(lambda x: x[0], ascending=False, numPartitions=1).collect())
    # 输出结果: [('n', 9), ('f', 1), ('e', 1), ('c', 3), ('c', 3), ('c', 5), ('b', 11), ('a', 1), ('a', 1)]
```
#### 2.4.14 soryByKey算子
* 功能：针对`KV型RDD`，对key进行排序，返回一个新的RDD
* 语法：`sortByKey(ascending=True, numPartitions=None, keyfunc=<function <lambda> at 0x0000020F4F4F5D08>)`
* 参数1：ascending=True表示升序，False表示降序
* 参数2：numPartitions=None表示排序后的RDD的分区数，如果不指定，默认和排序前的RDD的分区数一致
* 参数3：keyfunc=<function <lambda> at 0x0000020F4F4F5D08>表示排序依据，可以是数字，也可以是自定义的规则
```python
# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('E', 1), ('C', 1), ('D', 1), ('b', 1), ('g', 1), ('f', 1),
                          ('y', 1), ('u', 1), ('i', 1), ('o', 1), ('p', 1),
                          ('m', 1), ('n', 1), ('j', 1), ('k', 1), ('l', 1)], 3)

    print(rdd.sortByKey(ascending=True, numPartitions=1, keyfunc=lambda key: str(key).lower()).collect())
    # 输出结果: [('a', 1), ('b', 1), ('C', 1), ('D', 1), ('E', 1), ('f', 1), ('g', 1), ('i', 1), ('j', 1), ('k', 1), ('l', 1), ('m', 1), ('n', 1), ('o', 1), ('p', 1), ('u', 1), ('y', 1)]
```
### 2.5 常用Action算子
#### 2.5.1 countByKey算子
* 功能：统计每个key出现的次数（一般使用于kv型RDD）
* 语法：`countByKey()`
```python
# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.textFile("../data/input/words.txt")
    rdd2 = rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1))

    # 通过countByKey来对key进行计数, 这是一个Action算子
    result = rdd2.countByKey()

    print(result)
    # 输出结果: defaultdict(<class 'int'>, {'hello': 2, 'world': 2, 'spark': 1, 'hadoop': 1})
    print(type(result))
    # 输出结果: <class 'collections.defaultdict'>
```
#### 2.5.2 countByValue算子
* 功能：统计每个元素出现的次数
* 语法：`countByValue()`
```python
# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('c', 3), ('f', 1), ('b', 11), ('c', 3), ('a', 1), ('c', 5), ('e', 1), ('n', 9), ('a', 1)], 3)

    # 使用sortBy对rdd执行排序

    # 按照value 数字进行排序
    # 参数1函数, 表示的是 ,  告知Spark 按照数据的哪个列进行排序
    # 参数2: True表示升序 False表示降序
    # 参数3: 排序的分区数
    """注意: 如果要全局有序, 排序分区数请设置为1"""
    print(rdd.sortBy(lambda x: x[1], ascending=True, numPartitions=1).collect())
    # 输出结果: [('f', 1), ('a', 1), ('e', 1), ('c', 3), ('c', 3), ('c', 5), ('n', 9), ('b', 11)]
    
    # 按照key来进行排序
    print(rdd.sortBy(lambda x: x[0], ascending=False, numPartitions=1).collect())
    # 输出结果: [('n', 9), ('f', 1), ('e', 1), ('c', 3), ('c', 3), ('c', 5), ('b', 11), ('a', 1), ('a', 1)]
```
#### 2.5.3 collect算子
* 功能：将RDD各个分区内的数据，同一收集到Driver中，形成一个List对象
* 语法：`rdd.collect()`
* 注意：collect算子会将RDD中的数据全部加载到Driver端，如果数据量过大，可能会导致Driver端内存溢出

#### 2.5.4 reduce算子
* 功能：对RDD数据集按照你传入的逻辑进行聚合
* 语法：`rdd.reduce(func)`
* 参数：func是一个函数，接收两个参数，返回一个值，这个值会作为下一次调用func函数的第一个参数
```python
# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5])

    print(rdd.reduce(lambda a, b: a + b))
    # 输出结果: 15
```
#### 2.5.5 fold算子
* 功能：与reduce算子类似，接受传入逻辑进行聚合，只不过fold算子需要传入一个初始值
* 语法：`rdd.fold(zeroValue, func)`
* 这个初始值聚合，会作用在
  * 分区内聚合
  * 分区间聚合
```python
# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)

    print(rdd.fold(10, lambda a, b: a + b))
    # 输出结果: 85
```
#### 2.5.6 first算子
* 功能：返回RDD中的第一个元素
* 用法：`sc.parallelize([1, 2, 3, 4, 5]).first()`，返回1

#### 2.5.7 takeSample算子
* 功能：从RDD中随机抽取一定数量的元素
* 语法：`rdd.takeSample(withReplacement, num, seed)`
* 参数：
  * withReplacement：是否放回抽取，True表示放回抽取，False表示不放回抽取
  * num：抽取的数量
  * seed：随机数种子
```python
# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 5, 3, 1, 3, 2, 6, 7, 8, 6], 1)

    print(rdd.takeSample(False, 5, 1))
    # 输出结果: [3, 1, 3, 2, 6]
```
#### 2.5.8 takeOrdered算子
* 功能：返回RDD中排序后的前n个元素
* 语法：`rdd.takeOrdered(num, key)`
* 参数：
  * num：返回的元素个数
  * key：排序的key
```python
# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 2, 4, 7, 9, 6], 1)

    print(rdd.takeOrdered(3))
    # 输出结果: [1, 2, 3]
    print(rdd.takeOrdered(3, lambda x: -x))
    # 输出结果: [9, 7, 6]
```
#### 2.5.9 foreach算子
* 功能：遍历RDD中的每个元素，执行指定的逻辑
* 语法：`rdd.foreach(func)`
* 参数：func是一个函数，接收一个参数，没有返回值
* 注意：foreach算子是一个action算子，会触发作业的提交
```python
# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 2, 4, 7, 9, 6], 1)

    result = rdd.foreach(lambda x: print(x * 10))
    # 输出结果: 10 30 20 40 70 90 60
```
#### 2.5.10 saveAsTextFile算子
* 功能：将RDD中的数据保存到文件中
* 语法：`rdd.saveAsTextFile(path)`
* 参数：path是一个目录，如果目录存在，会报错
* 注意：saveAsTextFile算子是一个action算子，会触发作业的提交
```python
# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 2, 4, 7, 9, 6], 3)

    rdd.saveAsTextFile("hdfs://node1:8020/output/out1")
```
#### 2.5.11 mapPartitions算子
* 功能：将RDD中的每个分区的数据，传入到func函数中进行（整体分区）处理，返回一个新的RDD
* 语法：`rdd.mapPartitions(func)`
* 参数：func是一个函数，接收一个参数，返回一个可迭代对象
```python
# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 2, 4, 7, 9, 6], 3)

    def process(iter):
        result = list()
        for it in iter:
            result.append(it * 10)

        return result


    print(rdd.mapPartitions(process).collect())
    # 输出结果: [10, 30, 20, 40, 70, 90, 60]
```
#### 2.5.12 foreachPartition算子
* 功能：将RDD中的每个分区的数据，传入到func函数中进行处理
* 语法：`rdd.foreachPartition(func)`
* 参数：func是一个函数，接收一个参数，没有返回值
```python
# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 2, 4, 7, 9, 6], 3)

    def process(iter):
        result = list()
        for it in iter:
            result.append(it * 10)

        print(result)


    rdd.foreachPartition(process)
    # 输出结果: [10, 30]
    #         [20, 40]    
    #         [70, 90, 60]
```
#### 2.5.13 partitionBy算子
* 功能：将RDD中的数据按照指定的分区规则进行分区
* 语法：`rdd.partitionBy(numPartitions, partitionFunc)`
* 参数：
  * numPartitions：分区的数量
  * partitionFunc：分区规则
```python
# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('hadoop', 1), ('spark', 1), ('hello', 1), ('flink', 1), ('hadoop', 1), ('spark', 1)])

    # 使用partitionBy 自定义 分区
    def process(k):
        if 'hadoop' == k or 'hello' == k: return 0
        if 'spark' == k: return 1
        return 2


    print(rdd.partitionBy(3, process).glom().collect())
    # 输出结果: [[('hadoop', 1), ('hello', 1), ('hadoop', 1)], [('spark', 1), ('spark', 1)], [('flink', 1)]]
```
### 2.6 分区操作算子
#### 2.6.1 repartition算子 - Transformation
* 功能：对RDD重新分区（仅数量），会产生shuffle
* 语法：`rdd.repartition(numPartitions)`
* 参数：numPartitions：分区的数量
#### 2.6.2 coalesce算子 - Transformation
* 功能：对RDD重新分区（仅数量），不会产生shuffle
* 语法：`rdd.coalesce(numPartitions)`
* 参数：numPartitions：分区的数量
```python
# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5], 3)

    # repartition 修改分区
    print(rdd.repartition(1).getNumPartitions())

    print(rdd.repartition(5).getNumPartitions())

    # coalesce 修改分区
    print(rdd.coalesce(1).getNumPartitions())

    print(rdd.coalesce(5, shuffle=True).getNumPartitions())
```
### 2.7 面试题groupByKey和reduceByKey的区别
* 在功能上区别：
  * groupByKey：仅仅有分组功能
  * reduceByKey：除了有ByKey的分组功能，还有reduce聚合功能
* 在性能上区别：
  * groupByKey：会产生shuffle
  * reduceByKey：性能远大于`groupByKey + 聚合逻辑`
  * reduceByKey：在分组之前先聚合，减少了shuffle的数据量
分组+聚合，首选reduceByKey，数据越大，性能优势越明显
### 2.8 面试题map和mapPartitions的区别
* 在功能上区别：
  * map：一条数据处理一次
  * mapPartitions：一个分区的数据处理一次
  * mapPartitions：减少了函数调用的次数，提高了性能
* 在性能上区别：
  * map：性能低，因为每条数据都要调用一次函数
  * mapPartitions：性能高，因为每个分区调用一次函数
## DataFrame
      
### 创建DataFrame：
- 从现有数据源加载：您可以使用Spark提供的API从不同类型的数据源（如文件、数据库等）加载数据并创建DataFrame。例如，使用spark.read.csv()加载CSV文件、spark.read.json()加载JSON文件等。
- 从RDD转换：如果您已经有一个RDD（弹性分布式数据集），可以使用toDF()方法将其转换为DataFrame。
```python
# 从CSV文件加载DataFrame
df = spark.read.csv("file.csv", header=True, inferSchema=True)

# 从JSON文件加载DataFrame
df = spark.read.json("file.json")

# 从RDD创建DataFrame
rdd = spark.sparkContext.parallelize([(1, "Alice"), (2, "Bob")])
df = rdd.toDF(["id", "name"])
```
### 查看和检查DataFrame：
- show(): 用于查看DataFrame中的前几行数据，默认显示前20行。
- printSchema(): 打印DataFrame的模式（schema），包括每列的名称和数据类型信息。
- count(): 返回DataFrame中的行数。
- describe(): 提供DataFrame中数值列的基本统计信息，如均值、标准差、最小值和最大值。
```python
    # 显示DataFrame的前n行，默认n为20
df.show()

# 打印DataFrame的模式（列名称和数据类型）
df.printSchema()

# 获取DataFrame的行数
df.count()

# 获取DataFrame中数值列的基本统计信息
df.describe().show()
```
### 选择和过滤数据：
- select(): 选择DataFrame中的列。
- filter(): 过滤符合给定条件的行。
- where(): 过滤符合给定条件的行，与filter()功能相同。
- distinct(): 去除DataFrame中的重复行。
- orderBy(): 对DataFrame按照指定列进行排序。
```python
# 选择特定的列
df.select("name", "age")

# 过滤满足条件的行
df.filter(df.age > 30)

# 使用where()进行条件过滤
df.where(df.age > 30)
```
### 列操作：
- withColumn(): 添加新的列或替换现有列。
- drop(): 删除指定的列。
- cast(): 将列转换为不同的数据类型。
- alias(): 为列设置别名。
```python
 # 添加新列
df.withColumn("new_column", df.age + 1)

# 删除列
df.drop("column_name")

# 重命名列
df.withColumnRenamed("old_column", "new_column")
```
### 聚合和分组：
- groupBy(): 按照一个或多个列对DataFrame进行分组。
- agg(): 对分组后的数据进行聚合操作，如求和、平均值、最大值等。
- 支持的聚合函数：count()、sum()、avg()、min()、max()等。
```python
# 按照某一列进行分组，并计算每组的平均值
df.groupBy("category").avg("value")

# 多列分组，并计算每组的总和和最大值
df.groupBy("col1", "col2").agg({"value": "sum", "quantity": "max"})
```
### 排序
- 使用orderBy()方法按照指定的列对DataFrame进行排序
```python
# 按照age列升序排序
df.orderBy("age")

# 按照age列降序排序
df.orderBy(df.age.desc())
```
### 加入和连接：
- join(): 将两个DataFrame按照指定的列连接起来。
- union(): 将两个具有相同模式的DataFrame进行合并。
- intersect(): 获取两个DataFrame之间的交集。
- except(): 获取第一个DataFrame相对于第二个DataFrame的差集。
```python
# 基于共同列连接两个DataFrame
df1.join(df2, on="common_column")

# 合并两个DataFrame，要求具有相同的结构
df1.union(df2)
```
### 数据写入：
- write.format().save(): 将DataFrame写入指定的数据源，如Parquet、CSV、JSON等。
```python
# 将DataFrame写入Parquet文件
df.write.parquet("file.parquet")

# 将DataFrame写入CSV文件
df.write.csv("file.csv")

# 将DataFrame写入JSON文件
df.write.json("file.json")
```
