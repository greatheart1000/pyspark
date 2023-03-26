from pyspark import SparkConf,SparkContext
# distinct算子实现去重操作
if __name__ == '__main__':

    conf =SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd =sc.parallelize([2,3,6,8,10,12,1,4])
    print(rdd.takeOrdered(3))
    print(rdd.takeOrdered(3,lambda x:-x))
    """[1, 2, 3]
[12, 10, 8]"""