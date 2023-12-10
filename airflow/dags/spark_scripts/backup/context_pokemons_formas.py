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


dfResult = (dfPokemon
            .select('nome', fn.explode_outer('formas').alias('formas'), 'id', 'experiencia', 'altura', 'peso', 'type')
            .select('id', 'nome', 'experiencia', 'altura', 'peso','formas.name','formas.url', 'type')
            )

dfResult.printSchema()

dfResult.show(12,False)

(dfResult
 .write
 .format('parquet')
 .mode('overwrite')
 .save('s3a://context/pokemons_formas')
 )