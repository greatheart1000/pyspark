from pyspark import SparkConf,SparkContext
# distinct算子实现去重操作
if __name__ == '__main__':

    conf =SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9],1)
    print(rdd.takeSample(True, 22)) #[7, 5, 2, 4, 3, 3, 8, 9, 7, 9, 7, 8, 1, 5, 1, 5, 4, 7, 5, 1, 5, 7]

