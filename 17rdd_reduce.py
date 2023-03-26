from pyspark import SparkConf,SparkContext

#action 将 所有分区 聚合至一个list

# distinct算子实现去重操作
if __name__ == '__main__':

    conf =SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize(range(10))
    print(rdd.reduce(lambda a, b: a + b)) #45