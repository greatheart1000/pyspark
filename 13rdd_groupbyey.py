from pyspark import SparkConf,SparkContext
# distinct算子实现去重操作
if __name__ == '__main__':

    conf =SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd =sc.parallelize([("a",1),("b",1),("a",1),("b",1)
                         ])
    print(rdd.groupByKey().collect())
    #[('a', <pyspark.resultiterable.ResultIterable object at 0x7ff3942553c0>),
    # ('b', <pyspark.resultiterable.ResultIterable object at 0x7ff3942553f0>)]
    result= rdd.groupByKey()
    print(result.map(lambda x: (x[0], list(x[1]))).collect()) #[('a', [1, 1]), ('b', [1, 1])]
    