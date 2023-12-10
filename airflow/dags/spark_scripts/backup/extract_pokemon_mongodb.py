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

df = (spark
      .read
      .format("com.mongodb.spark.sql.DefaultSource")
      .option("database", "db_aulafia")
      .option("collection", "pokemon")
      .option("uri", "mongodb://aulafia:aulafia%40123@20.226.0.53:27017")
      .load()
)

df.printSchema()

df.show(12,False)

(df
 .write
 .format('parquet')
 .mode('overwrite')
 .save('s3a://raw/projeto/pokemon')
 )