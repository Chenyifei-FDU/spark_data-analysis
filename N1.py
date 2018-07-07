# coding=utf-8
from pyspark.sql import SQLContext, Row
from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    conf = SparkConf().set("spark.master", 'spark://10.190.2.112:7077').set('spark.app.name', 'task_14307110005') \
        .set('spark.default.parallelism', '15').set('spark.executor-cores', '2').set('spark.executor-memory', '8g') \
        .set('spark.num-executors', '3')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    sqlctx = SQLContext(sc)
    data = sc.textFile('hdfs://10.190.2.112/data/data_dump.txt')
    data = data.map(lambda line: line.split('\t'))

    data = data.map(lambda p: Row(gender=p[6], date_of_birth=p[8], last_name=p[3], nid=p[1]))

    dataset = sqlctx.createDataFrame(data)
    dataset.registerTempTable('Table')

    sql_N1_E = sqlctx.sql("select last_name, count(1) from Table where gender='E' group by last_name order by count(1) desc")
    sql_N1_K = sqlctx.sql("select last_name, count(1) from Table where gender='K' group by last_name order by count(1) desc")

    print('most common last name for Male:')
    sql_N1_E.show(10)
    print("most common last name for Female:")
    sql_N1_K.show(10)
