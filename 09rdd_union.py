from pyspark import SparkConf,SparkContext
# distinct算子实现去重操作
if __name__ == '__main__':

    conf =SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd1=sc.parallelize([1,2,3,4])
    rdd2 = sc.parallelize(['a', 'b','c', 'd'])
    rdd3=rdd1.union(rdd2)
    print(rdd3.collect())
    #[1, 2, 3, 4, 'a', 'b', 'c', 'd']
