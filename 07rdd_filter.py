from pyspark import SparkConf,SparkContext

if __name__ == '__main__':

    conf =SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([1,2,3,4,5,6,7,8,9])
    #通过filter算子 过滤奇偶数
    print(rdd.filter(lambda x: x % 2 == 0).collect()) #[2, 4, 6, 8]