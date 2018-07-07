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

    data = data.map(lambda p: Row(gender=p[6]))

    dataset = sqlctx.createDataFrame(data)
    dataset.registerTempTable('Table')

    answer_E5_1 = sqlctx.sql("select gender from Table where gender='E' ").count()
    answer_E5_2 = sqlctx.sql("select gender from Table where gender='K' ").count()
    print('Num of males:')
    print(answer_E5_1)
    print('Num of Females:')
    print(answer_E5_2)
    print('Proportion of men and women:{:4f}'.format(answer_E5_1/float(answer_E5_2)))
