from datetime import datetime

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import timezone

with DAG(
    dag_id="us_rt_container_stop",
    start_date=datetime(2026, 1, 1, tzinfo=timezone("America/New_York")),
    schedule="30 16 * * 1-5",  # 週一到週五 16:30 ET
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=1)},
    tags=["realtime", "us", "stock", "stop", "container"],
) as dag:

    stop_taskmanager = BashOperator(
        task_id="stop_taskmanager",
        bash_command="docker stop realtime-workspace-taskmanager-1",
        trigger_rule="all_done",
    )
    stop_jobmanager = BashOperator(
        task_id="stop_jobmanager",
        bash_command="docker stop realtime-workspace-jobmanager-1",
        trigger_rule="all_done",
    )
    stop_kafka_ui = BashOperator(
        task_id="stop_kafka_ui",
        bash_command="docker stop kafka-ui",
        trigger_rule="all_done",
    )
    stop_kafka = BashOperator(
        task_id="stop_kafka",
        bash_command="docker stop kafka0",
        trigger_rule="all_done",
    )

    stop_taskmanager >> stop_jobmanager >> stop_kafka_ui >> stop_kafka
