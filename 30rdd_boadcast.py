from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf =SparkConf().setAppName("test").setMaster("local[*]")
    sc =SparkContext(conf=conf)
    stu_list =[
        (1,"张大仙",11),
        (2, "pdd", 13),
        (3, "55",3),
        (4, "666", 14)
    ]
    broadcast = sc.broadcast(stu_list)

    rdd = score_info_rdd = sc.parallelize([(3,'英语',99),
                                     (4, '编程', 99),
                                       (1, '语文', 99),
                                     (2, '数学', 99),
                                     ( 1,'语文',99),
                                     (2, "编程", 99),
                                     (3,'语文',99),
                                     (4, '英语', 99),
                                     (1, '语文', 99)])

    def map_func(data):
        id = data[0]
        name =""
        for i in  broadcast.value:
            if id==i[0]:
                name = i[1]
        return (name,data[1],data[2])

    print(rdd.map(lambda x: map_func(x)).collect())

