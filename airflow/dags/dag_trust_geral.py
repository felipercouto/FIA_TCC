import os
from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators.daily_task_sensor import DailyExternalTaskSensor
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

os.environ["JAVA_HOME"] = Variable.get('JAVA_HOME')

default_args = {
    'owner': 'aulafia',
    'start_date': datetime(2023, 5, 28)
}

dag = DAG(dag_id='dag_trust_geral',
          default_args=default_args,
          schedule_interval='0 3 * * *',
          tags=['TRUST']
      )

start_dag = DummyOperator(
                task_id='start_dag',
                dag=dag
                )

sensor_context_geral_cadastro = DailyExternalTaskSensor(
                    task_id = 'sensor_context_geral_cadastro',
                    external_dag_id = 'dag_context_geral',
                    external_task_id = 'context_cadastro',
                    dag = dag
                    )

sensor_context_geral_visitas = DailyExternalTaskSensor(
                    task_id = 'sensor_context_geral_visitas',
                    external_dag_id = 'dag_context_geral',
                    external_task_id = 'context_visitas',
                    dag = dag
                    )

sensor_context_geral_prescricao = DailyExternalTaskSensor(
                    task_id = 'sensor_context_geral_prescricao',
                    external_dag_id = 'dag_context_geral',
                    external_task_id = 'context_prescricao',
                    dag = dag
                    )


spark_qualifica_medico = SparkSubmitOperator(
                          task_id='trust_qualifica_medico',
                          conn_id='spark_local',
                          jars='/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
                                /usr/local/airflow/jars/hadoop-aws-3.2.2.jar,\
                                /usr/local/airflow/jars/postgresql-42.3.3.jar'.replace(' ', ''),
                          application='/usr/local/airflow/dags/spark_scripts/trust_qualifica_medico.py',
                          dag=dag
                      )

spark_analise_especialidade = SparkSubmitOperator(
                          task_id='trust_analise_especialidade',
                          conn_id='spark_local',
                          jars='/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
                                /usr/local/airflow/jars/hadoop-aws-3.2.2.jar,\
                                /usr/local/airflow/jars/postgresql-42.3.3.jar'.replace(' ', ''),
                          application='/usr/local/airflow/dags/spark_scripts/trust_analise_especialidade.py',
                          dag=dag
                      )

spark_analise_estado = SparkSubmitOperator(
                          task_id='trust_analise_estado',
                          conn_id='spark_local',
                          jars='/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
                                /usr/local/airflow/jars/hadoop-aws-3.2.2.jar,\
                                /usr/local/airflow/jars/postgresql-42.3.3.jar'.replace(' ', ''),
                          application='/usr/local/airflow/dags/spark_scripts/trust_analise_estado.py',
                          dag=dag
                      )


dag_finish = DummyOperator(
                 task_id='dag_finish',
                 dag=dag
                 )

start_dag >> sensor_context_geral_cadastro >> sensor_context_geral_visitas >> sensor_context_geral_prescricao >> [spark_qualifica_medico,spark_analise_especialidade,spark_analise_estado] >> dag_finish