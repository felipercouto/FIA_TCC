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

dfPokemonFormas = (spark
                  .read
                  .format('parquet')
                  .load('s3a://context/pokemons_formas')
            )

dfResult = (dfPokemonFormas
            .groupBy("type")
            .agg(fn.avg("peso").cast('integer').alias("media_peso"),
                  fn.min("peso").alias("min_peso"),
                  fn.max("peso").alias("max_peso"),
                  fn.avg("altura").cast('integer').alias("media_altura"),
                  fn.min("altura").alias("min_altura"),
                  fn.max("altura").alias("max_altura"),
                  fn.count("id").alias("qtd_total"))
            .orderBy('qtd_total', ascending=False)
            )
            

dfResult.printSchema()

dfResult.show(12,False)

(dfResult
 .write
 .format('parquet')
 .mode('overwrite')
 .save('s3a://trust/types')
 )