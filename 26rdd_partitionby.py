from pyspark import SparkConf,SparkContext
# distinct算子实现去重操作
if __name__ == '__main__':

    conf =SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([("a", 1), ("a", 11), ("a", 10), ("a", 5),
                          ("a", 6), ("e", 1), ("e", 12), ("e", 2)])
    def process(k):
        if "a"==k :return 0
        if "e"==k:return 1
    #自定义分区
    print(rdd.partitionBy(3, process).glom().collect())
