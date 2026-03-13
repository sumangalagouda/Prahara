# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from airflow.sensors.filesystem import FileSensor

# default_args = {
#     'owner': 'data_engineer',
#     'depends_on_past': False,
#     'start_date': datetime(2026, 3, 13),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# dag = DAG(
#     'event_driven_data_pipeline',
#     default_args=default_args,
#     description='Event-driven pipeline - triggers when new CSV appears',
#     schedule_interval=None,  # only triggered by FileSensor
#     catchup=False,
# )

# # Watches data/input/ — triggers the moment a CSV appears
# file_sensor = FileSensor(
#     task_id='wait_for_new_file',
#     filepath='/opt/airflow/data/input/titanic.csv',
#     fs_conn_id='fs_default',
#     poke_interval=30,       # checks every 30 seconds
#     timeout=60 * 60 * 24,  # gives up after 24 hours
#     mode='poke',
#     dag=dag,
# )

# ingest_bronze = BashOperator(
#     task_id='ingest_to_bronze',
#     bash_command='cd /opt/airflow && python spark/bronze_job.py',
#     dag=dag,
# )

# bronze_to_silver = BashOperator(
#     task_id='bronze_to_silver',
#     bash_command='cd /opt/airflow && python spark/silver_job.py',
#     dag=dag,
# )

# silver_to_gold = BashOperator(
#     task_id='silver_to_gold',
#     bash_command='cd /opt/airflow && python spark/gold_job.py',
#     dag=dag,
# )

# # Chain — FileSensor must pass before anything runs
# file_sensor >> ingest_bronze >> bronze_to_silver >> silver_to_gold

# ============================================================
# DAG 2: Event-Driven Data Pipeline
# Automatically triggers when ANY new CSV appears in input dir
# ============================================================

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
import os
import glob

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'event_driven_data_pipeline',
    default_args=default_args,
    description='Event-driven pipeline - triggers when ANY new CSV appears in input dir',
    schedule_interval='* * * * *',  # Polls every  min — no manual trigger needed
    catchup=False,
    max_active_runs=1,                # One file processed at a time, no race conditions
)

INPUT_DIR = '/opt/airflow/data/input'
PROCESSED_DIR = '/opt/airflow/data/processed'


# ── STEP 1: Scan directory for any CSV ───────────────────────
def check_for_any_csv(**context):
    """
    Scans INPUT_DIR for any .csv file.
    - Returns False (skips pipeline) if none found.
    - Pushes chosen filename to XCom if found.
    - Processes oldest file first (FIFO). Swap min() → max() for newest-first.
    """
    csv_files = glob.glob(os.path.join(INPUT_DIR, '*.csv'))
    if not csv_files:
        print("No CSV files found in input directory. Skipping pipeline.")
        return False  # ShortCircuitOperator skips all downstream tasks

    chosen_file = min(csv_files, key=os.path.getmtime)  # Oldest first (FIFO)
    print(f"Found CSV: {chosen_file}")
    context['ti'].xcom_push(key='csv_file', value=chosen_file)
    return True


check_for_csv = ShortCircuitOperator(
    task_id='check_for_new_csv',
    python_callable=check_for_any_csv,
    provide_context=True,
    dag=dag,
)


# ── STEP 2: Bronze ingestion — receives detected filename ─────
ingest_bronze = BashOperator(
    task_id='ingest_to_bronze',
    bash_command=(
        'cd /opt/airflow && '
        'python spark/bronze_job.py '
        '--input "{{ ti.xcom_pull(task_ids=\'check_for_new_csv\', key=\'csv_file\') }}"'
    ),
    dag=dag,
)


# ── STEP 3: Silver transformation ────────────────────────────
bronze_to_silver = BashOperator(
    task_id='bronze_to_silver',
    bash_command='cd /opt/airflow && python spark/silver_job.py',
    dag=dag,
)


# ── STEP 4: Gold aggregation ─────────────────────────────────
silver_to_gold = BashOperator(
    task_id='silver_to_gold',
    bash_command='cd /opt/airflow && python spark/gold_job.py',
    dag=dag,
)


# ── STEP 5: Archive processed file ───────────────────────────
def archive_file(**context):
    """
    Moves processed CSV → /processed/filename_YYYYMMDD_HHMMSS.csv
    Prevents the same file from re-triggering the pipeline.
    """
    csv_file = context['ti'].xcom_pull(task_ids='check_for_new_csv', key='csv_file')
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    basename = os.path.basename(csv_file)
    name, ext = os.path.splitext(basename)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    dest = os.path.join(PROCESSED_DIR, f'{name}_{timestamp}{ext}')

    os.rename(csv_file, dest)
    print(f"Archived: {csv_file} → {dest}")


cleanup = PythonOperator(
    task_id='archive_processed_file',
    python_callable=archive_file,
    provide_context=True,
    dag=dag,
)


# ── Chain ────────────────────────────────────────────────────
# Scans dir → (no file: skip all) OR (file found: full pipeline) → archive
check_for_csv >> ingest_bronze >> bronze_to_silver >> silver_to_gold >> cleanup