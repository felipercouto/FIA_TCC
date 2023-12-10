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

df_visitas = (spark
             .read
             .format('parquet')
             .load('s3a://raw/visitas')
       )

df_visitas = df_visitas.withColumn("DATA_VISITA", fn.to_date(fn.substring(fn.col("DATA_DA_VISITA"),1,10)))

dfResult = (df_visitas
    .filter(df_visitas.VISITA_EFETIVA == "S")
    .select(fn.concat(df_visitas.NUM_CRM,df_visitas.UF_CRM).alias("CRM"), "NUM_CRM", "UF_CRM", "DATA_VISITA", "ID_VISITA", "PROF_ID", "DATA_INCLUSAO", "TIPO_VISITA", "CUSTOMER", fn.year(df_visitas.DATA_DA_VISITA).alias("ANO"), fn.month(df_visitas.DATA_DA_VISITA).alias("MES"))
    )


dfResult.printSchema()

dfResult.show(12,False)

(dfResult
 .write
 .format('parquet')
 .mode('overwrite')
 .save('s3a://context/visitas')
 )


