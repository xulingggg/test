from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.master('local').appName('test').getOrCreate()

sc = spark.sparkContext

# when run spark job, log number is large, set the log level to ERROR.
sc.setLogLevel('ERROR')

schema = StructType(
    [
        StructField('Uno', StringType()),
        StructField('Dos', StringType()),
        StructField('Tres', StringType()),
        StructField('Cuatro', StringType())
    ]
)

lzt = [
    Row('one', 'two', 'three', 'four'),
    Row('eins', 'zwei', 'drei', 'vier')
]

rdd1 = sc.parallelize(lzt)

rdd1.foreach(print)

rdd1.toDF(schema).show()
