#"Оркестрация загрузок"


#В Apache Airflow необходимо создать:
#Соединение gp_std<номер пользователя> с Greenplum 
#Directed Acyclic Graph с названием std<номер пользователя>_main dag, который будет состоять из следующих Task'ов:
#Цикличная загрузка справочных данных в целевые таблицы из внешних  таблиц. 
#Загрузка данных для целевых таблиц фактов из внешних таблиц.
#Расчёт витрины.
#DAG должен удалённо запускать в Greenplum пользовательские функции, которые были написаны ранее.
#После написания Python кода необходимо проверить работоспособность и корректность выполнения DAG'а.


from airflow import DAG
from datetime import datetime, timedelta 
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator 
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

#load_table = "select std11_26.f_load_simple('std11_26.plan', 'gp.plan', 'intern', 'intern')"



db_conn = "gp_sapiens_std11_26"
db_schema = 'std11_26'

#загрузка таблиц-фактов из базы данных postgres в Greenplum
db_fact_load = 'f_load_simple'
parent_tables = ['plan', 'sales']
facts_table_load_query = f"select {db_schema}.{db_fact_load}(%(tab_name)s, %(parent_tab)s, 'intern', 'intern');"


#загрузка таблиц-справочников из файлов, которые находятся на локальном диске с пом. gpfdist
db_proc_load = 'f_load_full'
full_load_tables = ['region', 'chanel', 'price', 'product']
full_load_files = {'region':'region',
                  'chanel':'chanel',
                  'price':'price',
                  'product':'product'
                  }
md_table_load_query = f"select {db_schema}.{db_proc_load}(%(tab_name)s, %(file_name)s);"

#Создание витрины
#make_mart = 'f_task_mart'
#mart_month = '202105'
#mart_query = f"select {db_schema}.{make_mart}(%(month)s);"
make_mart = "select std11_26.f_task_mart('202105')"

default_args = {
    'depends_on_past': False,
    'owner': 'std11_26',
    'start_date': datetime(2025, 4, 23),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    "task_dag_1",
    max_active_runs=3,
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:

    task_start = DummyOperator(task_id="start")

#    task_part = PostgresOperator(task_id="start_insert_fact",
#                                        postgres_conn_id="gp_sapiens_std11_26",
#                                        sql=load_table
#                                        )

#Блок задач по загрузке таблиц фактов. С циклом, т.к. таблиц несколько.
    with TaskGroup("facts_insert") as task_facts_insert_tables:
        for table in parent_tables:
            task = PostgresOperator(task_id=f"load_table_{table}",
                                    postgres_conn_id=db_conn,
                                    sql=facts_table_load_query,
                                    parameters={'tab_name':f'{db_schema}.{table}', 'parent_tab':f'gp.{table}'}
                                    )


#Блок задач по загрузке справочников. С циклом, т.к. таблиц несколько.
    with TaskGroup("full_insert") as task_full_insert_tables:
        for table in full_load_tables:
            task = PostgresOperator(task_id=f"load_table_{table}",
                                    postgres_conn_id=db_conn,
                                    sql=md_table_load_query,
                                    parameters={'tab_name':f'{db_schema}.{table}', 'file_name':f'{full_load_files[table]}'}
                                    )

            
#Блок задач запуска функции создания витрины.
    task_mart = PostgresOperator(task_id="start_mart",
                                  postgres_conn_id=db_conn,
                                  sql=make_mart #,
                                 # parameters = {'month':f'{mart_month}'}
                                  )

    
    task_end = DummyOperator(task_id="end")
    
    task_start >> task_facts_insert_tables >> task_full_insert_tables >> task_mart >> task_end

