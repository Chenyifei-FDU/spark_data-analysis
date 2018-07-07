from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession as spark
from operator import add


def get_age(date_of_birth):
    year = date_of_birth.split('/')[-1]
    try:
        year = int(year)
        return 2018 - int(year)
    except:
        return 'NULL'


def get_bucket(age):
    if 0 <= age <= 18:
        return '0-18'
    elif 19 <= age <= 28:
        return '19-28'
    elif 29 <= age <= 38:
        return '29-38'
    elif 49 <= age <= 55:
        return '49-55'
    elif age > 60:
        return '>60'


if __name__ == '__main__':
    conf = SparkConf().set("spark.master", 'spark://10.190.2.112:7077').set('spark.app.name', 'task_14307110005') \
        .set('spark.default.parallelism', '15').set('spark.executor-cores', '2').set('spark.executor-memory', '8G') \
        .set('spark.num-executors', '3')
    sc = SparkContext(conf=conf)

    data = sc.textFile('hdfs://10.190.2.112/data/data_dump.txt')
    data = data.map(lambda line: line.strip().split('\t'))
    data = data.map(lambda line: line[8])
    data = data.map(get_age).map(get_bucket).map(lambda x: (x, 1)).reduceByKey(add)
    print(sorted(data.collect(), key=lambda x: x[0]))
