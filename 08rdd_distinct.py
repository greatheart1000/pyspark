from pyspark import SparkConf,SparkContext
# distinct算子实现去重操作
if __name__ == '__main__':

    conf =SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([1,1,1,2,2,4,3,3])

    print(rdd.distinct().collect()) #[1, 2, 4, 3]
    rdd2 = sc.parallelize([("a",1),("b",1),("c",1),("a",1)])
    print(rdd2.distinct().collect()) #[('a', 1), ('b', 1), ('c', 1)]