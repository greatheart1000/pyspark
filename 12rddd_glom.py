from pyspark import SparkConf,SparkContext
# distinct算子实现去重操作
if __name__ == '__main__':

    conf =SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    #glom算子的作用是加上嵌套 查看分区
    rdd = sc.parallelize([1,2,3,5,4,6,7,8,9],3)
    print(rdd.glom().collect()) #[[1, 2, 3], [5, 4, 6], [7, 8, 9]]
    #解嵌套
    print(rdd.glom().flatMap(lambda x: x).collect())
    #[1, 2, 3, 5, 4, 6, 7, 8, 9]
