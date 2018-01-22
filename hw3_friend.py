
# coding: utf-8

# In[ ]:
import re
import sys
from pyspark import SparkConf, SparkContext
from itertools import combinations
conf = SparkConf()
sc = SparkContext(conf=conf)
lines = sc.textFile(sys.argv[1])
user_friend = lines.map(lambda l:tuple(l.split('\t')))
user_ratingprod = user_friend.flatMap(lambda x:[((x[0],i),-1000) for i in x[1].split(',')])
u_friend = user_friend.map(lambda x:(x[1].split(',')))
user_ratingprod2 = u_friend.flatMap(lambda w:[(i,1) for i in combinations(w,2)])
unio = user_ratingprod.union(user_ratingprod2).reduceByKey(lambda n1,n2:n1+n2).map(lambda x: (x[0][0],(x[0][0],x[0][1],x[1])))
result = unio.filter(lambda u:u[1][2] >= 0).groupByKey().map(lambda v: sorted(v[1], key=lambda k: k[2], reverse=True)).map(lambda x: x[0:10])
result.saveAsTextFile(sys.argv[2])
sc.stop()


#~/opt/spark/bin/spark-submit count_words.py hw3_friend.txt ~/spark_code/final/

