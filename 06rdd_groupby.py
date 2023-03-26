from pyspark import SparkConf,SparkContext

if __name__ == '__main__':

    conf =SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([('a',1),('b',1),('a',1),('b',1),('a',1),('b',1)])
    #通过 groupby对数据进行分组
    #print(rdd.groupBy(lambda x: x[0]).collect())
    #[('a', <pyspark.resultiterable.ResultIterable object at 0x7fd2347333d0>),
    # ('b', <pyspark.resultiterable.ResultIterable object at 0x7fd234733700>)]
    result= rdd.groupBy(lambda x: x[0])
    print(result.map(lambda x: (x[0], list(x[1]))).collect())
    #[('a', [('a', 1), ('a', 1), ('a', 1)]), ('b', [('b', 1), ('b', 1), ('b', 1)])]