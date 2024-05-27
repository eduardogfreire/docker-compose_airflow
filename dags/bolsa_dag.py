from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from sqlalchemy import create_engine
import requests
import pandas as pd
import os

stock = pd.read_csv('/opt/airflow/arquivos/stock.csv', squeeze=True).tolist()

token = '7Xw7wxMo7sSY59wHapFCUg'

def bolsa_de_valores(stock, token, **context):
    try:
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

        execution_date = context['ds']  # Execution date in YYYY-MM-DD format
        final_df['created_at'] = execution_date  # Adiciona a coluna 'created_at'

        filename = f'/tmp/bolsa_de_valores/resultado_{execution_date}.csv'
        final_df.to_csv(filename, index=False)

        context['task_instance'].xcom_push(key='file_path', value=filename)
    except Exception as e:
        print(f"Erro ao obter dados da API ou salvar em CSV: {e}")
        raise

def salvar_em_csv(**context):
    try:
        file_path = context['task_instance'].xcom_pull(task_ids='pega_task', key='file_path')
        if file_path:
            df = pd.read_csv(file_path)
            df.to_csv(file_path, index=False)
            print(f"Arquivo salvo em {file_path}")
        else:
            print("Nenhum resultado para salvar.")
    except Exception as e:
        print(f"Erro ao salvar CSV: {e}")
        raise

def import_csv_to_mysql(**context):
    try:
        execution_date = context['ds']  # Execution date in YYYY-MM-DD format
        filename = f'/tmp/bolsa_de_valores/resultado_{execution_date}.csv'
        df = pd.read_csv(filename)
        
        # Adiciona a coluna 'created_at' ao DataFrame
        df['created_at'] = execution_date

        engine = create_engine('mysql://root:senha@172.22.0.4:3306/database_airflow')
        df.to_sql('acoes_bolsa_brapi', con=engine, if_exists='append', index=False)
    except Exception as e:
        print(f"Erro ao importar CSV para MySQL: {e}")
        raise

create_table_sql = """
CREATE TABLE IF NOT EXISTS acoes_bolsa_brapi (
    id INT AUTO_INCREMENT PRIMARY KEY,
    created_at DATETIME,
    currency VARCHAR(255),
    shortName VARCHAR(255),
    longName VARCHAR(255),
    regularMarketChange DECIMAL(18, 2),
    regularMarketChangePercent DECIMAL(18, 2),
    regularMarketTime VARCHAR(255),
    regularMarketPrice DECIMAL(18, 2),
    regularMarketDayHigh INT,
    regularMarketDayRange VARCHAR(255),
    regularMarketDayLow INT,
    regularMarketVolume BIGINT,
    regularMarketPreviousClose DECIMAL(18, 2),
    regularMarketOpen DECIMAL(18, 2),
    fiftyTwoWeekRange VARCHAR(255),
    fiftyTwoWeekLow INT,
    fiftyTwoWeekHigh INT,
    symbol VARCHAR(255),
    summaryProfile TEXT,
    priceEarnings DECIMAL(18, 2),
    earningsPerShare DECIMAL(18, 2),
    logourl VARCHAR(255),
    UNIQUE(symbol, id)
);
"""

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
    'teste_bolsa_valores_dag',
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

    create_table_task = MySqlOperator(
        task_id='create_table_task',
        mysql_conn_id='mysqlconn',  # Nome da conexão definida no Airflow
        sql=create_table_sql,
    )

    import_csv_task = PythonOperator(
        task_id='import_csv_to_mysql',
        python_callable=import_csv_to_mysql,
        provide_context=True,
    )

    end_task = DummyOperator(task_id='end_task')

    start_task >> check_pip >> update_pip >> check_pandas >> install_pandas
    install_pandas >> cria_diretorio_task >> pega_task >> salvar_task >> create_table_task
    create_table_task >> import_csv_task >> end_task
