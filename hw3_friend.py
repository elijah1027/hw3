
# coding: utf-8

# In[ ]:


import re
import sys
from pyspark import SparkConf, SparkContext
conf = SparkConf()
sc = SparkContext(conf=conf)
lines = sc.textFile(sys.argv[1])
user_friend = lines.map(lambda l:tuple(l.split('\t')))
user_ratingprod = user_friend.flatMap(lambda x:[((x[0],i),-1000) for i in x[1].split(',')])
user_ratingprod2 = user_friend.flatMap(lambda x: (lambda j:[[((j[u],j[i]),1) for i in range(u+1,len(j))]for u in range(0,len(j))])(x[1].split(',')))
user_ratingprod2 = user_ratingprod2.flatMap(lambda xs: [x for x in xs])
unio = user_ratingprod.union(user_ratingprod2).reduceByKey(lambda n1,n2:n1+n2)
unio = unio.map(lambda x: (x[0][0],x[0],x[1]))
key = unio.map(lambda x: (x[0]))
lst = key.collect()
uni_list = []
for i in lst:
    if i not in uni_list:
        uni_list.append(i)
charRDD = []

for i in uni_list:
    char = i
    user = unio.filter(lambda x: x[0]==char)
    top_10 = user.filter(lambda u:u[2] >= 0).takeOrdered(10, key=lambda x: -x[2])
    if len(top_10) == 10:
        charRDD.append(top_10)
    else:
        user_friend = user.map(lambda x:x[1][1]).collect()
        
        for j in uni_list:
            if j not in user_friend:
                top_10.append((char,(char,j),"random friend"))
                if len(top_10) == 10:
                    charRDD.append(top_10)
                    break

charRDD = sc.parallelize(charRDD)
charRDD.saveAsTextFile(sys.argv[2])
sc.stop()

