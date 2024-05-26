from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import requests
import pandas as pd
import os

stock = pd.read_csv('/opt/airflow/arquivos/stock.csv', squeeze=True).tolist()

token = '7Xw7wxMo7sSY59wHapFCUg'

def bolsa_de_valores(stock, token, **context):
    dfs = []
    
    for symbol in stock:
        r = requests.get(f'https://brapi.dev/api/quote/{symbol}?modules=summaryProfile&token={token}')
        data = r.json()
        if 'results' in data:
            df = pd.DataFrame(data['results'])
            if not df.empty:
                dfs.append(df)
    
    if dfs:
        final_df = pd.concat(dfs, ignore_index=True)
    else:
        final_df = pd.DataFrame()
    
    # Generate the filename with date
    execution_date = context['ds']  # Execution date in YYYY-MM-DD format
    filename = f'/tmp/bolsa_de_valores/resultado_{execution_date}.csv'
    
    final_df.to_csv(filename, index=False)
    
    # Push the file path to XCom
    context['task_instance'].xcom_push(key='file_path', value=filename)

def salvar_em_csv(**context):
    # Pull the file path from XCom
    file_path = context['task_instance'].xcom_pull(task_ids='pega_task', key='file_path')
    
    if file_path:
        df = pd.read_csv(file_path)
        df.to_csv(file_path, index=False)
        print(f"Arquivo salvo em {file_path}")
    else:
        print("Nenhum resultado para salvar.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    'bolsa_valores_dag',
    default_args=default_args,
    description='Uma DAG instala: pip, biblioteca pandas do python e após consulta a api salvando em csv',
    schedule_interval=timedelta(days=1),
) as dag:
    
    check_pip = BashOperator(
        task_id='check_pip',
        bash_command='command -v pip || echo "pip not found"',
        retries=0
    )

    update_pip = BashOperator(
        task_id='update_pip',
        bash_command='pip install --upgrade pip',
        retries=0
    )

    check_pandas = BashOperator(
        task_id='check_pandas',
        bash_command='python -c "import pandas" || exit 1',
        retries=0
    )

    install_pandas = BashOperator(
        task_id='install_pandas',
        bash_command='pip install pandas',
        retries=0
    )

    start_task = DummyOperator(task_id='start_task')

    cria_diretorio_task = BashOperator(
        task_id='cria_diretorio',
        bash_command='mkdir -p /tmp/bolsa_de_valores',
    )

    pega_task = PythonOperator(
        task_id='pega_task',
        python_callable=bolsa_de_valores,
        op_kwargs={'stock': stock, 'token': token},
        provide_context=True,
        dag=dag,
    )

    salvar_task = PythonOperator(
        task_id='salvar_em_csv',
        python_callable=salvar_em_csv,
        provide_context=True,
        dag=dag,
    )

    end_task = DummyOperator(task_id='end_task')


    start_task >> check_pip >> update_pip >> check_pandas >> install_pandas
    install_pandas >> cria_diretorio_task >> pega_task >> salvar_task >> end_task
