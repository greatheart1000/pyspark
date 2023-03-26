from pyspark import SparkConf,SparkContext
import re
if __name__ == '__main__':
    conf =SparkConf().setAppName("test").setMaster("local[*]")
    sc =SparkContext(conf=conf)
    """hadoop spark # hadoop spark sparkmapreduce ! spark spark hive !
    hive spark hadoop mapreduce spark %
    spark hive sql sql spark hive , hive spark !!
    hdfs hdfs mapreduce mapreduce spark hive
    #"""
    # 需求: 1.正常单词 有多少个 2.特殊字符有多少个
    abnormal_char =[ ",",".","!","%","#","$"]
    file_rdd =sc.textFile('./data/text1.txt')
    #1.将 abnormal_char包装成广播变量
    broadcast = sc.broadcast(abnormal_char)
    #2.对特殊字符做累加，用累加器
    num = sc.accumulator(0)
    #3 .对文件进行处理
    lines_rdd = file_rdd.filter(lambda x:x.strip()) #去除空行
    #4.去除前后空行
    lines_rdd = lines_rdd.map(lambda x:x.strip())
    words_rdd = lines_rdd.map(lambda data:re.split("\s+",data))
    def filter_word(data):
        global num
        if data in broadcast.value:
            num+=1
            return False
        return True
    normal_word =words_rdd.filter(filter_word)
    #得到正常单词
    results = normal_word.map(lambda a,b:a+b)

    res =words_rdd.map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b)
    print(res.collect())
    print(num)


