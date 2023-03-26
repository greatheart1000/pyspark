from pyspark import SparkConf,SparkContext
# distinct算子实现去重操作
if __name__ == '__main__':

    conf =SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.textFile('./data/words.txt')
    rdd2=rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x,1))
    # countByKey()
    result = rdd2.countByKey()
    print(result)
    print(type(result ))
    """defaultdict(<class 'int'>, {'hello': 2, '': 1, 'hadoop': 2, 'yes': 1, 'pyspark': 1})
<class 'collections.defaultdict'>
"""