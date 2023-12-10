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

dag = DAG(dag_id='dag_context_geral',
          default_args=default_args,
          schedule_interval='0 3 * * *',
          tags=['CONTEXT']
      )

start_dag = DummyOperator(
                task_id='start_dag',
                dag=dag
                )

spark_cadastro = SparkSubmitOperator(
                          task_id='context_cadastro',
                          conn_id='spark_local',
                          jars='/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
                                /usr/local/airflow/jars/hadoop-aws-3.2.2.jar,\
                                /usr/local/airflow/jars/postgresql-42.3.3.jar'.replace(' ', ''),
                          application='/usr/local/airflow/dags/spark_scripts/context_cadastro.py',
                          dag=dag
                      )

spark_visitas = SparkSubmitOperator(
                          task_id='context_visitas',
                          conn_id='spark_local',
                          jars='/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
                                /usr/local/airflow/jars/hadoop-aws-3.2.2.jar,\
                                /usr/local/airflow/jars/postgresql-42.3.3.jar'.replace(' ', ''),
                          application='/usr/local/airflow/dags/spark_scripts/context_visitas.py',
                          dag=dag
                      )

spark_prescricao = SparkSubmitOperator(
                          task_id='context_prescricao',
                          conn_id='spark_local',
                          jars='/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
                                /usr/local/airflow/jars/hadoop-aws-3.2.2.jar,\
                                /usr/local/airflow/jars/postgresql-42.3.3.jar'.replace(' ', ''),
                          application='/usr/local/airflow/dags/spark_scripts/context_prescricao.py',
                          dag=dag
                      )

dag_finish = DummyOperator(
                 task_id='dag_finish',
                 dag=dag
                 )


#start_dag >> [sensor_pokemon,sensor_type] >> [spark_task_moves, spark_task_formas] >> dag_finish
#start_dag >> sensor_pokemon >> sensor_type >> [spark_task_moves,spark_task_formas] >> dag_finish
start_dag >> [spark_cadastro,spark_visitas,spark_prescricao] >> dag_finish