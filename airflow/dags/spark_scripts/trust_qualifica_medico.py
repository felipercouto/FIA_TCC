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

# Definição dos critérios de qualidade
completude_weight = 10
consistencia_weight = 5
data_atualizacao_weight = 2
ultima_prescricao_weight = 5
atualizacao_cadastro_weight = 3
ultima_visita_weight = 3

df_cadastro = (df_cadastro
     .join(df_visitas, df_cadastro.CRM == df_visitas.CRM, "inner")
     .join(df_presc, (df_visitas.CRM == df_presc.CRM) & (df_visitas.ANO == df_visitas.ANO) & (df_visitas.MES == df_visitas.MES), "inner")
     .select(df_cadastro.CRM, df_cadastro.COD_REG, df_cadastro.UF_REG, df_cadastro.NOME, df_cadastro.SEXO, df_cadastro.ESPECIALIDADE_1, df_cadastro.ESPECIALIDADE_2, df_cadastro.LOCAL_TRABALHO, df_cadastro.ENDERECO, df_cadastro.NUMERO, df_cadastro.COMPLEMENTO, df_cadastro.CEP, df_cadastro.BAIRRO, df_cadastro.MUNICIPIO, df_cadastro.ESTADO, df_cadastro.PROF_ID, df_cadastro.DSC_PRIMEIRA_ESPECIALIDADE, df_cadastro.DATA_INCLUSAO, df_cadastro.COD_CATEGORIA, df_cadastro.CATEGORIA, df_cadastro.CUSTOMER, df_visitas.DATA_VISITA, df_presc.DATA_PRESC)
#     .filter(sf.col("ANO") == 2023)
     .groupby("CRM", "COD_REG", "UF_REG", "NOME", "SEXO", "ESPECIALIDADE_1", "ESPECIALIDADE_2", "LOCAL_TRABALHO", "ENDERECO", "NUMERO", "COMPLEMENTO", "CEP", "BAIRRO", "MUNICIPIO", "ESTADO", "PROF_ID", "DSC_PRIMEIRA_ESPECIALIDADE", "DATA_INCLUSAO", "COD_CATEGORIA", "CATEGORIA", "CUSTOMER").agg(sf.max("DATA_VISITA").alias("DATA_ULT_VISITA"), sf.max("DATA_PRESC").alias("DATA_ULT_PRESC"))
)

# Calcula a pontuação para cada critério
medicos_score_df = df_cadastro.withColumn("completude_score", (sf.col("CRM").isNotNull().cast("int") +
                                                            sf.col("NOME").isNotNull().cast("int") +
                                                            sf.col("ESPECIALIDADE_1").isNotNull().cast("int")))


# Consistência do campo "crm": vamos verificar se o "crm" possui 7 dígitos, que é um formato comum no Brasil
medicos_score_df = medicos_score_df.withColumn("consistencia_score", sf.when(sf.col("CRM").rlike("^[0-9]{7}$"), 1).otherwise(0))

# Verificar se o endereço está completo (logradouro, numero, bairro, cidade, estado e cep)
medicos_score_df = medicos_score_df.withColumn("endereco_completo_score", (sf.col("ENDERECO").isNotNull().cast("int") +
                                                                            sf.col("NUMERO").isNotNull().cast("int") +
                                                                            sf.col("BAIRRO").isNotNull().cast("int") +
                                                                            sf.col("MUNICIPIO").isNotNull().cast("int") +
                                                                            sf.col("ESTADO").isNotNull().cast("int") +
                                                                            sf.col("CEP").isNotNull().cast("int")))

# Calcular a data atual como referência para calcular a pontuação da última prescrição, data de atualização, data da última visita e vínculo no CNES
data_referencia = sf.to_date(sf.lit("2023-08-24"))

# Calcular a diferença em dias entre a data de atualização do cadastro e a data de referência
medicos_df = df_cadastro.withColumn("dias_atualizacao", sf.datediff(data_referencia, sf.to_date(sf.col("DATA_INCLUSAO"), "yyyy-MM-dd")))

# Normalizar a diferença em dias para a pontuação (quanto mais recente, maior a pontuação)
max_dias_atualizacao = medicos_df.agg({"dias_atualizacao": "max"}).collect()[0][0]
medicos_df = medicos_df.withColumn("atualizacao_cadastro_score", sf.col("dias_atualizacao") / max_dias_atualizacao)

# Calcular a pontuação final da data de atualização do cadastro (com peso aplicado)
medicos_df = medicos_df.withColumn("atualizacao_cadastro_score", sf.col("atualizacao_cadastro_score") * atualizacao_cadastro_weight)


# Converter a coluna "data_prescricao" para formato de data
medicos_df = medicos_df.withColumn("DATA_ULT_PRESC", sf.to_date(sf.col("DATA_ULT_PRESC"), "yyyy-MM-dd"))

# Calcular a diferença em dias entre a data da última prescrição e a data de referência
medicos_df = medicos_df.withColumn("dias_ultima_prescricao", sf.datediff(data_referencia, sf.col("DATA_ULT_PRESC")))

# Normalizar a diferença em dias para a pontuação (quanto mais recente, maior a pontuação)
max_dias_ultima_prescricao = medicos_df.agg({"dias_ultima_prescricao": "max"}).collect()[0][0]
medicos_df = medicos_df.withColumn("ultima_prescricao_score", sf.col("dias_ultima_prescricao") / max_dias_ultima_prescricao)

# Calcular a pontuação final da última prescrição (com peso aplicado)
medicos_df = medicos_df.withColumn("ultima_prescricao_score", sf.col("ultima_prescricao_score") * ultima_prescricao_weight)


# Calcular a diferença em dias entre a data da última visita e a data de referência
medicos_df = medicos_df.withColumn("dias_ultima_visita", sf.datediff(data_referencia, sf.col("DATA_ULT_VISITA")))

# Normalizar a diferença em dias para a pontuação (quanto mais recente, maior a pontuação)
max_dias_ultima_visita = medicos_df.agg({"dias_ultima_visita": "max"}).collect()[0][0]
medicos_df = medicos_df.withColumn("ultima_visita_score", sf.col("dias_ultima_visita") / max_dias_ultima_visita)

# Calcular a pontuação final da data da última visita (com peso aplicado)
medicos_df = medicos_df.withColumn("ultima_visita_score", sf.col("ultima_visita_score") * ultima_visita_weight)


# Unir os dataframes para obter a pontuação da última prescrição, data de atualização, data da última visita e vínculo no CNES para cada médico
medicos_score_df = medicos_score_df.join(medicos_df.select("CRM", "ultima_prescricao_score", "atualizacao_cadastro_score", "ultima_visita_score"), on="CRM", how="left")

medicos_score_df = medicos_score_df.withColumn("data_atualizacao_score", (sf.col("DATA_INCLUSAO") == "2023-08-24").cast("int"))


# Calcula a pontuação total
medicos_score_df = medicos_score_df.withColumn("pontuacao_total", completude_weight * sf.col("completude_score") +
                                                            consistencia_weight * sf.col("consistencia_score") +
                                                            data_atualizacao_weight * sf.col("data_atualizacao_score") +
                                                            consistencia_weight * sf.col("endereco_completo_score") +
                                                            ultima_prescricao_weight * sf.col("ultima_prescricao_score") +
                                                            atualizacao_cadastro_weight * sf.col("atualizacao_cadastro_score") +
                                                            ultima_visita_weight * sf.col("ultima_visita_score"))



# Classifica os médicos com base na pontuação total em ordem decrescente
medicos_classificados_df = medicos_score_df.orderBy(sf.col("pontuacao_total").desc())





medicos_classificados_df.printSchema()

medicos_classificados_df.show(12,False)

(medicos_classificados_df
 .write
 .format('parquet')
 .mode('overwrite')
 .save('s3a://trust/medicos_classificados')
 )