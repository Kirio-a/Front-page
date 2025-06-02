from airflow import DAG
from datetime import datetime, timedelta, date
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup




DB_CONN = "gp_std11_36"
DB_SCHEMA = "std11_36"


DB_PROC_LOAD_FULL = 'f_load_full'
FULL_LOAD_TABLES = ['region', 'chanel', 'price', 'product']
FULL_LOAD_FILES = {'region':'region_ext', 'chanel':'chanel_ext', 'price':'price_ext', 'product':'product_ext'}


DB_PROC_LOAD_PARTITION = 'f_load_simple_partition'
PARTITION_LOAD_TABLES = ['plan', 'sales']
PARTITION_LOAD_EXT_TABLES = {'plan':'plan_ext', 'sales':'sales_ext'}
PARTITION_LOAD_PERIODS = [
    "2021-01-01", "2021-02-01", "2021-03-01", "2021-04-01",
    "2021-05-01", "2021-06-01", "2021-07-01"
]


DB_PROC_CREATE_MARTS_AND_VIEWS = 'create_sales_performance_marts'

default_args = {
    'depends_on_past': False, 
    'owner': 'std11_36',  
    'start_date': datetime(2025, 4, 22),  
    'retries': 1,  
    'retry_delay': timedelta(minutes=5)  
}
   
with DAG(
    'std11_36_main_dag', 
    max_active_runs=3,  
    schedule_interval=None,  
    default_args=default_args,  
    catchup=False  # Если DAG пропустил запуски, они не будут выполняться задним числом
) as dag: 
    
    task_start = DummyOperator(task_id="start")
    
    # Группа задач для загрузки данных в справочные таблицы
    with TaskGroup("full_insert") as task_full_insert_tables:  # Группируем задачи под именем "full_insert"
        for table in FULL_LOAD_TABLES:  # Перебираем список справочных таблиц
            task = PostgresOperator(
                    task_id=f"load_table_{table}",  
                    postgres_conn_id=DB_CONN,  
                    sql=f"""
                        select {DB_SCHEMA}.{DB_PROC_LOAD_FULL}(
                       '{table}', 
                       '{FULL_LOAD_FILES[table]}'
                        );
                        """
                )
    
    
    # Группа задач для загрузки партиционированных данных
    with TaskGroup("partition_insert") as task_partition_insert_tables:
        for table in PARTITION_LOAD_TABLES:
            for period in PARTITION_LOAD_PERIODS:
                task = PostgresOperator(
                    task_id=f"load_partition_{table}_{period}",
                    postgres_conn_id=DB_CONN,
                    sql=f"""
                        SELECT {DB_SCHEMA}.{DB_PROC_LOAD_PARTITION}(
                            '{DB_SCHEMA}.{table}', 
                            'date', 
                            '{period}'::timestamp, 
                            '{DB_SCHEMA}.{PARTITION_LOAD_EXT_TABLES[table]}'
                        );
                    """
                )

     # Группа задач для создания витрин и представлений для каждого загруженного периода
    with TaskGroup("marts_and_views_creation") as task_create_marts_and_views:
        for period in PARTITION_LOAD_PERIODS:
            period_cut=period[:4]+period[5:7]
            task = PostgresOperator(
                task_id=f"create_mart_and_view_{period}",
                postgres_conn_id=DB_CONN,
                sql=f"""SELECT {DB_SCHEMA}.{DB_PROC_CREATE_MARTS_AND_VIEWS}('{period_cut}');"""
                )
                
    
    task_end = DummyOperator(task_id="end")
    
task_start >> [task_full_insert_tables, task_partition_insert_tables] >> task_create_marts_and_views >> task_end

