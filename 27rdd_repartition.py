from pyspark import SparkConf,SparkContext
# distinct算子实现去重操作
if __name__ == '__main__':
    #repartition 重新分区
    conf =SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([1,2,3,4,5,6,7,8,9],3)
    print(rdd.repartition(5).getNumPartitions())
    print(rdd.repartition(1).getNumPartitions())
    """5
        1"""