from pyspark.sql.types import *
import pyspark.sql.functions as sf
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

df_cadastro = (spark
             .read
             .format('parquet')
             .load('s3a://raw/cadastro')
       )

#dfResult = dfCadastro.select(fn.concat(dfCadastro.COD_REG,dfCadastro.UF_REG).alias("CRM"),"COD_REG", "UF_REG", "NOME", "SEXO", "ESPECIALIDADE_1", "ESPECIALIDADE_2", "LOCAL_TRABALHO", "TIPO_LOGRAD", "ENDERECO", "NUMERO", "COMPLEMENTO", "CEP", "BAIRRO", "MUNICIPIO", "ESTADO", "PROF_ID", "DSC_PRIMEIRA_ESPECIALIDADE", "DATA_INCLUSAO", "COD_CATEGORIA", "CATEGORIA", "CUSTOMER")

df_cadastro = df_cadastro.select(sf.concat(df_cadastro.COD_REG,df_cadastro.UF_REG).alias("CRM"),"COD_REG", "UF_REG", "NOME", "SEXO", "ESPECIALIDADE_1", "ESPECIALIDADE_2", "LOCAL_TRABALHO", "TIPO_LOGRAD", "ENDERECO", "NUMERO", "COMPLEMENTO", "CEP", "BAIRRO", "MUNICIPIO", "ESTADO", "PROF_ID", "DSC_PRIMEIRA_ESPECIALIDADE", "DATA_INCLUSAO", "COD_CATEGORIA", "CATEGORIA", "CUSTOMER")
df_cadastro = df_cadastro.fillna({'TIPO_LOGRAD':' '})
df_cadastro = df_cadastro.select(sf.concat(df_cadastro.TIPO_LOGRAD,sf.lit(" "),df_cadastro.ENDERECO).alias("ENDERECO"), "CRM","COD_REG", "UF_REG", "NOME", "SEXO", "ESPECIALIDADE_1", "ESPECIALIDADE_2", "LOCAL_TRABALHO", "NUMERO", "COMPLEMENTO", "CEP", "BAIRRO", "MUNICIPIO", "ESTADO", "PROF_ID", "DSC_PRIMEIRA_ESPECIALIDADE", "DATA_INCLUSAO", "COD_CATEGORIA", "CATEGORIA", "CUSTOMER")
df_cadastro = df_cadastro.select("CRM", "COD_REG", "UF_REG", "NOME", "SEXO", "ESPECIALIDADE_1", "ESPECIALIDADE_2", "LOCAL_TRABALHO", "ENDERECO", "NUMERO", "COMPLEMENTO", "CEP", "BAIRRO", "MUNICIPIO", "ESTADO", "PROF_ID", "DSC_PRIMEIRA_ESPECIALIDADE", "DATA_INCLUSAO", "COD_CATEGORIA", "CATEGORIA", "CUSTOMER")


df_cadastro.printSchema()

df_cadastro.show(12,False)

(df_cadastro
 .write
 .format('parquet')
 .mode('overwrite')
 .save('s3a://context/cadastro')
 )


