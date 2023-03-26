#flatmap的功能数是解除嵌套，首先map 然后解除嵌套

from pyspark import SparkConf,SparkContext


if __name__ == '__main__':
    conf = SparkConf().setAppName('test').setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd =sc.parallelize(['a b','a a'])  #[1,2,3],[1,2,3],[1,2,3]
    # 方法1print(rdd.map(lambda x: x.split()).collect())
    print(rdd.flatMap(lambda x:x.split()).collect())  # 方法2