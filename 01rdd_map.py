from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('test').setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([1,2,3,4,5,6],3)
    def add(a):
        return a*10
    print(rdd.map(add).collect())
    print(rdd.map(lambda x: x * 10).collect())
