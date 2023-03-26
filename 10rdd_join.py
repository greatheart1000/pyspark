from pyspark import SparkConf,SparkContext
# distinct算子实现去重操作
if __name__ == '__main__':

    conf =SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    #join算子 实现关联
    rdd=sc.parallelize([(1001,"zhangsan"),(1002,"lisi"),
                    (1001,"wangwu"),(1001,"maliu")])
    rdd1=sc.parallelize([(1001,"销售部"),(1002,"科技部")])
    #通过i哦join 实现rdd之间的关联
    print(rdd.join(rdd1).collect())
    #[(1002, ('lisi', '科技部')), (1001, ('zhangsan', '销售部')),
    # (1001, ('wangwu', '销售部')), (1001, ('maliu', '销售部'))]
    print(rdd.leftOuterJoin(rdd1).collect())