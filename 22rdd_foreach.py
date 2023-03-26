from pyspark import SparkConf,SparkContext
# distinct算子实现去重操作
if __name__ == '__main__':

    conf =SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([2, 3, 6, 8, 10, 12, 1, 4])
    #foreach没有返回值
    result =rdd.foreach(lambda x:print(x*10))
    print(result)