# coding=utf-8

from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Window, functions


# Reference:https://stackoverflow.com/questions/46008057/pyspark-top-for-dataframe
def topK_df(df, key_col, K):
    """
    Using window functions.  Handles ties OK.
    """
    window = Window.orderBy(functions.col(key_col).desc())
    return (df
            .withColumn("rank", functions.rank().over(window))
            .filter(functions.col('rank') <= K)
            .drop('rank'))


if __name__ == '__main__':
    conf = SparkConf().set("spark.master", 'spark://10.190.2.112:7077').set('spark.app.name', 'task_14307110005') \
        .set('spark.default.parallelism', '15').set('spark.executor-cores', '2').set('spark.executor-memory', '8g') \
        .set('spark.num-executors', '3')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    spark = SparkSession(sc)

    data = sc.textFile('hdfs://10.190.2.112/data/data_dump.txt')
    data = data.map(lambda x: x.split('\t'))
    data = data.map(lambda line: (line[2], line[6]))

    schema = StructType([
        StructField('name', StringType(), False),
        StructField('gender', StringType()),

    ])

    table = spark.createDataFrame(data, schema)
    table.createOrReplaceTempView('Table')

    print('for male:')
    spark.sql(
        "select name, count(1) from Table where gender='E' group by name order by count(1) desc").show(5)

    print('for female:')
    spark.sql(
        "select name, count(1) from Table where gender='K' group by name order by count(1) desc").show(5)
