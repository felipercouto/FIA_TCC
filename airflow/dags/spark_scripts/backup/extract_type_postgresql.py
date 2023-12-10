from pyspark.sql.types import *
import pyspark.sql.functions as fn
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
         .config("spark.hadoop.fs.s3a.access.key", "aulafia")
         .config("spark.hadoop.fs.s3a.secret.key", "aulafia@123")
         .config("spark.hadoop.fs.s3a.path.style.access", True)
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
         .getOrCreate()
        )

schema = ArrayType(
                   StructType([
                               StructField('name', StringType(), True),
                               StructField('url', StringType(), True)
                              ])
                  )

df = (spark
      .read
      .format('jdbc')
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://20.226.0.53:5432/db_aulafia")
      .option("dbtable", "db_aulafia.type")
      .option("user", "aulafia")
      .option("password", "aulafia@123")
      .load()
      .withColumn('moves', fn.from_json('moves', schema))
)

df.printSchema()

df.show(12,False)

(df
 .write
 .format('parquet')
 .mode('overwrite')
 .save('s3a://raw/postgres/type')
 )