# coding=utf-8

from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Window, functions


if __name__ == '__main__':
    conf = SparkConf().set("spark.master", 'spark://10.190.2.112:7077').set('spark.app.name', 'task_14307110005') \
        .set('spark.default.parallelism', '15').set('spark.executor-cores', '2').set('spark.executor-memory', '8g') \
        .set('spark.num-executors', '3')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    spark = SparkSession(sc)

    data = sc.textFile('hdfs://10.190.2.112/data/data_dump.txt')
    data = data.map(lambda x: x.split('\t'))
    data = data.map(lambda line: (line[0], line[2], line[11]))

    schema = StructType([
        StructField('uid', StringType(), False),
        StructField('name', StringType()),
        StructField('city', StringType(), True)
    ])


    table = spark.createDataFrame(data, schema)
    table.createOrReplaceTempView('Table')
    spark.sql('''
    select * from
            (select city,name,nb_name,rank() over(partition by city order by nb_name desc) as rk
            from
                (select city,name,count(name) as nb_name
                from
                    (select b.city,b.name
                    from
                        (select city, count(city) as num
                        from Table
                        group by city
                        order by num desc
                        limit 10
                        ) a
                    join Table b
                    on a.city=b.city
                    )
                group by city,name
                )
            )
            where rk<=3
            ''').show(30)


