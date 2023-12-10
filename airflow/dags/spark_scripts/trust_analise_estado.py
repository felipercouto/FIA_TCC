from pyspark.sql.types import *
import pyspark.sql.functions as fn
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

spark = (SparkSession.builder
         .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
         .config("spark.hadoop.fs.s3a.access.key", "aulafia")
         .config("spark.hadoop.fs.s3a.secret.key", "aulafia@123")
         .config("spark.hadoop.fs.s3a.path.style.access", True)
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
         .getOrCreate()
        )

dfCadastro = (spark
                  .read
                  .format('parquet')
                  .load('s3a://context/cadastro')
            )

df_visitas = (spark
                  .read
                  .format('parquet')
                  .load('s3a://context/visitas')
            )


df_presc = (spark
                  .read
                  .format('parquet')
                  .load('s3a://context/prescricao')
            )


dfResult2023 = (dfCadastro
     .join(df_visitas, dfCadastro.CRM == df_visitas.CRM, "inner")
     .join(df_presc, (df_visitas.CRM == df_presc.CRM) & (df_visitas.ANO == df_visitas.ANO) & (df_visitas.MES == df_visitas.MES), "inner")
     .select(dfCadastro.CRM, dfCadastro.ESTADO, df_visitas.ID_VISITA, df_presc.PX1, df_presc.ANO, df_presc.MES)
     .filter(fn.col("ANO") == 2023)
     .groupby("ESTADO", "ANO", "MES").agg(fn.countDistinct("CRM").alias("QTD_CRM_2023"), fn.count("ID_VISITA").alias("QTD_VISITA_2023"), fn.sum("PX1").alias("PX_2023"))
)

dfResult2022 = (dfCadastro
     .join(df_visitas, dfCadastro.CRM == df_visitas.CRM, "inner")
     .join(df_presc, (df_visitas.CRM == df_presc.CRM) & (df_visitas.ANO == df_visitas.ANO) & (df_visitas.MES == df_visitas.MES), "inner")
     .select(dfCadastro.CRM, dfCadastro.ESTADO, df_visitas.ID_VISITA, df_presc.PX1, df_presc.ANO, df_presc.MES)
     .filter(fn.col("ANO") == 2022)
     .groupby("ESTADO", "ANO", "MES").agg(fn.countDistinct("CRM").alias("QTD_CRM_2022"), fn.count("ID_VISITA").alias("QTD_VISITA_2022"), fn.sum("PX1").alias("PX_2022"))
)            

dfResult = (dfResult2023
    .join(dfResult2022, (dfResult2023.ESTADO == dfResult2022.ESTADO) & (dfResult2023.MES == dfResult2022.MES), "full")
    .withColumn('PERCAPTA_2023', fn.col("PX_2023") / fn.col("QTD_CRM_2023"))
    .withColumn('PERCAPTA_2022', fn.col("PX_2022") / fn.col("QTD_CRM_2022"))
    .withColumn('SHARE_2023',fn.col('PX_2023')/fn.sum('PX_2023').over(Window.partitionBy(dfResult2023.MES))*100)
    .withColumn('SHARE_2022',fn.col('PX_2022')/fn.sum('PX_2022').over(Window.partitionBy(dfResult2022.MES))*100)
    .select(dfResult2023.ESTADO, dfResult2023.MES, dfResult2023.QTD_CRM_2023, dfResult2023.QTD_VISITA_2023, dfResult2023.PX_2023, fn.round('PERCAPTA_2023').alias("PERCAPTA_2023"), fn.round('SHARE_2023').alias("SHARE_2023"), dfResult2022.QTD_CRM_2022, dfResult2022.QTD_VISITA_2022, dfResult2022.PX_2022, fn.round('PERCAPTA_2022').alias("PERCAPTA_2022"), fn.round('SHARE_2022').alias("SHARE_2022"))
    .orderBy("QTD_CRM_2023", ascending=False)
)

dfResult = (dfResult
     .withColumn('CRESCIMENTO_SHARE', fn.col("SHARE_2023") - fn.col("SHARE_2022"))
)

dfResult.printSchema()

dfResult.show(12,False)

(dfResult
 .write
 .format('parquet')
 .mode('overwrite')
 .save('s3a://trust/analise_estado')
 )