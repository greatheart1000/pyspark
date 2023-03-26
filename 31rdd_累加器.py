from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf =SparkConf().setAppName("test").setMaster("local[*]")
    sc =SparkContext(conf=conf)
    rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10],2)
    """spark有累加器 """
    acmlt = sc.accumulator(0)

    count =0

    def map_func(data):
        global acmlt
        acmlt  += 1
        print(acmlt)
    result = rdd.map(map_func).collect()
    print(acmlt)
    rdd3 =result.map(lambda x:x)
    rdd3.collect()
    print(acmlt)