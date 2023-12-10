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

df_presc = (spark
             .read
             .format('parquet')
             .load('s3a://raw/prescricao')
       )

df_presc = df_presc.withColumn("DATA_PRESC", fn.to_date(fn.substring(fn.col("DATA"),1,10)))

dfResult = (df_presc
     .filter(df_presc.PX1 > 0)
     .select("DATA_PRESC", "CRM", "CLASSE", "PX1", fn.year(df_presc.DATA).alias("ANO"), fn.month(df_presc.DATA).alias("MES"))
    )


dfResult.printSchema()

dfResult.show(12,False)

(dfResult
 .write
 .format('parquet')
 .mode('overwrite')
 .save('s3a://context/prescricao')
 )


