# coding=utf-8

from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *


def get_age(date_of_birth):
    year = date_of_birth.split('/')[-1]
    try:
        year = int(year)
        return 2018 - int(year)
    except:
        return 0


if __name__ == '__main__':
    conf = SparkConf().set("spark.master", 'spark://10.190.2.112:7077').set('spark.app.name', 'task_14307110005') \
        .set('spark.default.parallelism', '15').set('spark.executor-cores', '2').set('spark.executor-memory', '8g') \
        .set('spark.num-executors', '3')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    spark =SparkSession(sc)

    data = sc.textFile('hdfs://10.190.2.112/data/data_dump.txt')
    data = data.map(lambda line: line.split('\t'))

    data = data.map(lambda line: (get_age(line[8]), line[11]))
    schema = StructType([
        StructField('age', IntegerType(), True),
        StructField('city', StringType(), True)
    ])
    age_city = spark.createDataFrame(data, schema)
    age_city.createOrReplaceTempView('Table')

    print(spark.sql("select city, avg(age) from Table group by city").show(100,False))

    print("5 city with smallest average age:")
    print(spark.sql("select city, avg(age) from Table group by city order by avg(age)").show(5))
