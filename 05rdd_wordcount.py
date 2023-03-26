from pyspark import SparkConf,SparkContext

if __name__ == '__main__':

    conf =SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    file = sc.textFile('./data/words.txt')
    words =file.flatMap(lambda x:x.split(" "))
    #3.flatMap取出所有单词

    # 4. key 是单词 value 是1
    word_with= words.map(lambda word:(word,1))
    #用这个进行分组聚合
    print(word_with.reduceByKey(lambda a, b: a + b).collect())
