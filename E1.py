from pyspark.sql import SQLContext, Row
from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    conf = SparkConf().set("spark.master", 'spark://10.190.2.112:7077').set('spark.app.name', 'task_14307110005') \
        .set('spark.default.parallelism', '15').set('spark.executor-cores', '2').set('spark.executor-memory', '8G') \
        .set('spark.num-executors', '3')
    sc = SparkContext(conf=conf)

    sqlctx = SQLContext(sc)
    data = sc.textFile('hdfs://10.190.2.112/data/data_dump.txt')
    data = data.map(lambda line: line.split('\t'))

    data = data.map(lambda p: Row(gender=p[6], DOB=p[8].split('/')[2]+p[8].split('/')[1]+p[8].split('/')[0] ,name=p[2] + ' ' + p[3], nid=p[1]))

    dataset = sqlctx.createDataFrame(data)
    dataset.registerTempTable('Table')

    answer_E1 = sqlctx.sql("select name, gender, DOB from Table where gender = 'E' order by DOB")
    answer_E1.show(10)