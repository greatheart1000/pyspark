from pyspark import SparkConf,SparkContext
# distinct算子实现去重操作
if __name__ == '__main__':

    conf =SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    #返回交集
    rdd1=sc.parallelize([('a',1),('b',1),('c',1)])
    rdd2 = sc.parallelize([('a', 1), ('d', 1), ('c', 1)])
    rdd3= rdd1.intersection(rdd2)
    print(rdd3.collect()) #[('a', 1), ('c', 1)]