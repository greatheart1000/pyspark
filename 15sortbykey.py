from pyspark import SparkConf,SparkContext
# distinct算子实现去重操作
if __name__ == '__main__':

    conf =SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([("a", 1), ("b", 11), ("c", 10), ("x", 5),
                          ("g", 6), ("m", 1), ("k", 12), ("h", 2)])
    print(rdd.sortByKey(ascending=True, numPartitions=2).collect())
    #[('a', 1), ('b', 11), ('c', 10), ('g', 6), ('h', 2), ('k', 12), ('m', 1), ('x', 5)]
    