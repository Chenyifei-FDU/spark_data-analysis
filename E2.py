from pyspark.sql import SQLContext, Row
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession as spark
from operator import add

if __name__ == '__main__':
    conf = SparkConf().set("spark.master", 'spark://10.190.2.112:7077').set('spark.app.name', 'task_14307110005') \
        .set('spark.default.parallelism', '15').set('spark.executor-cores', '2').set('spark.executor-memory', '8G') \
        .set('spark.num-executors', '3')
    sc = SparkContext(conf=conf)

    # sqlctx = SQLContext(sc)
    # data = spark.read.csv('hdfs://10.190.2.112/data/data_dump.txt',header=False,sep='\t')
    # table = spark.createDataFrame()
    data = sc.textFile('hdfs://10.190.2.112/data/data_dump.txt')
    data = data.map(lambda line: line.split('\t'))
    data = data.flatMap(lambda line: list(line[2]+line[3])).map(lambda char: (char,1)).reduceByKey(add)
    print(sorted(data.collect(),key=lambda kv: kv[1],reverse=True))

    # dataset = sqlctx.createDataFrame(data)
    # dataset.registerTempTable('Table')
    #     #
    #     # answer_E2 = sqlctx.sql("select character, count(character) from Table group by character order by count(character), desc")
    #     # answer_E2.show(10)
    sc.stop()