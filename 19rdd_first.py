from pyspark import SparkConf,SparkContext
# distinct算子实现去重操作
if __name__ == '__main__':

    conf =SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    print(sc.parallelize([1, 2, 3]).first())
    #take取前面的几个
    print(sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9]).take(5)) #[1, 2, 3, 4, 5]

    #top  取最大的  [9, 8, 7]

    print(sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9]).top(3))
