from pyspark import SparkConf,SparkContext
# distinct算子实现去重操作
if __name__ == '__main__':

    conf =SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd =sc.parallelize([1,2,3,4,5,6,7,8,9],3)
    print(rdd.fold(10, lambda a, b: a + b)) #85
    #print(rdd.glom().collect()) #[[1, 2, 3], [4, 5, 6], [7, 8, 9]]