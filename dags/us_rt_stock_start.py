from datetime import datetime

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import timezone

with DAG(
    dag_id="us_rt_stock_start",
    start_date=datetime(2026, 1, 1, tzinfo=timezone('America/New_York')),
    schedule="20 9 * * 1-5",        # 週一到週五 09:20 ET
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=1)},
    tags=["realtime","us","stock"],
) as dag:

    start_producer = BashOperator(
        task_id="start_producer",
        bash_command="docker start kafka-producer",
    )

    start_consumer = BashOperator(
        task_id="start_consumer",
        bash_command="docker start kafka-consumer",
    )

    start_producer 
    start_consumer

