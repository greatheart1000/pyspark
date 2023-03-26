from pyspark import SparkConf,SparkContext
# distinct算子实现去重操作
if __name__ == '__main__':

    conf =SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    #sortby算子
    rdd = sc.parallelize([("a",1),("a",11),("a",10),("a",5),
                    ("a",6),("e",1),("e",12),("e",2)])
    #ascending=True 升序，numPartitions=2分2个区
    print(rdd.sortBy(lambda x: x[1], ascending=True,
                     numPartitions=2).collect())
    #[('a', 1), ('e', 1), ('e', 2), ('a', 5), ('a', 6),
    # ('a', 10), ('a', 11), ('e', 12)]