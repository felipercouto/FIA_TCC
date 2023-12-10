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

dfPokemon = (spark
             .read
             .format('parquet')
             .load('s3a://raw/postgres/pokemon')
       )

dfType = (spark
          .read
          .format('parquet')
          .load('s3a://raw/postgres/type')
         )

dfResult = (dfPokemon.join(dfType, dfType.name == dfPokemon.type, how='inner')
            .select('*', fn.explode_outer('moves').alias('move'))
            .select('id', 'nome', 'type', fn.col('move.name').alias('move'))
            )

dfResult.printSchema()

dfResult.show(12,False)

(dfResult
 .write
 .format('parquet')
 .mode('overwrite')
 .save('s3a://context/pokemons_moves')
 )


