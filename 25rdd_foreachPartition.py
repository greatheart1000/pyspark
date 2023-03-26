from pyspark import SparkConf,SparkContext
# distinct算子实现去重操作
if __name__ == '__main__':

    conf =SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd=sc.parallelize([1,2,3,45,6,7,8,11,12],3)
    def process(iter):
        result=[]
        for data in iter:
            result.append(data*10)
            print(result)


    print(rdd.foreachPartition(process))