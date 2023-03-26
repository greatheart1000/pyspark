#coding:utf8
from pyspark import SparkConf,SparkContext
import jieba
if __name__ == '__main__':
    content ="小明硕士毕业于中国科学院计算所"
    print(list(jieba.cut(content))) #['小明', '硕士', '毕业', '于', '中国科学院', '计算所']

    print(list(jieba.cut(content,False))) #['小明', '硕士', '毕业', '于', '中国科学院', '计算所']
    print(list(jieba.cut_for_search(content)))
    #['小明', '硕士', '毕业', '于', '中国', '科学', '学院', '科学院', '中国科学院', '计算', '计算所']