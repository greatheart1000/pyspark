from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf =SparkConf().setAppName('test').setMaster("local[*]")
    sc = SparkContext(conf=conf)

    #mapValues
    rdd = sc.parallelize([('a',1),('b',1),('a',1),('b',1),('a',1),('b',1)])
    print(rdd.mapValues(lambda x: x *10 ).collect())#只针对values
    #[('a', 10), ('b', 10), ('a', 10), ('b', 10), ('a', 10), ('b', 10)]
