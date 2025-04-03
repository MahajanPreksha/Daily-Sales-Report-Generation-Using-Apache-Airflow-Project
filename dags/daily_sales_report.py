from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import os
import csv
import logging
from airflow.exceptions import AirflowException
from airflow.models.param import Param


def _transform_and_load_data_(**kwargs):
    ti = kwargs['ti']
    sales_date = kwargs['params'].get('sales_date')
    if not sales_date:
        raise AirflowException('Sales date param is required!')
    
    logging.info(f"Transforming data for sales date: {sales_date}")

    pg_hook = PostgresHook(postgres_conn_id='postgres_localhost')

    transform_query = '''
        INSERT INTO daily_sales_reports (report_date, total_sales, total_quantity, average_price, number_of_transactions, unique_products_sold, highest_selling_product_id, highest_selling_product_name)
        SELECT 
            %s AS report_date,
            SUM(price * quantity) AS total_sales,
            SUM(quantity) AS total_quantity,
            AVG(price) AS average_price,
            COUNT(*) AS number_of_transactions,
            COUNT(DISTINCT product_id) AS unique_products_sold,
            (SELECT product_id FROM sales_data WHERE sale_date = %s ORDER BY price * quantity DESC LIMIT 1) as highest_selling_product_id,
            (SELECT product_name FROM sales_data WHERE sale_date = %s ORDER BY price * quantity DESC LIMIT 1) as highest_selling_product_name
        FROM sales_data
        WHERE sale_date = %s
        ON CONFLICT (report_date) DO UPDATE SET
            total_sales = EXCLUDED.total_sales,
            total_quantity = EXCLUDED.total_quantity,
            average_price = EXCLUDED.average_price,
            number_of_transactions = EXCLUDED.number_of_transactions,
            unique_products_sold = EXCLUDED.unique_products_sold,
            highest_selling_product_id = EXCLUDED.highest_selling_product_id,
            highest_selling_product_name = EXCLUDED.highest_selling_product_name;
    '''
    pg_hook.run(transform_query, parameters=(sales_date, sales_date, sales_date, sales_date))

    logging.info("Data tranformation completed successfully!")

    transformed_query = '''
        SELECT *
        FROM daily_sales_reports
        WHERE report_date = %s;
    '''

    transformed_data = pg_hook.get_records(transformed_query, parameters=(sales_date,))

    ti.xcom_push(key='transformed_data', value=transformed_data)

    logging.info("Data loaded successfully!")

    return True

def _generate_report_(**kwargs):
    ti = kwargs['ti']
    sales_date = kwargs['params'].get('sales_date')
    if not sales_date:
        raise AirflowException('Sales date param is required!')
    logging.info(f"Loading data for sales date: {sales_date}")

    report_dir = os.path.join(os.path.dirname(__file__), 'daily_sales_reports')
    os.makedirs(report_dir, exist_ok=True)

    pg_hook = PostgresHook(postgres_conn_id='postgres_localhost')

    report_query = '''
        SELECT * FROM daily_sales_reports
        WHERE report_date = %s;
    '''
    report_data = pg_hook.get_records(report_query, parameters=(sales_date,))

    report_file_path = os.path.join(report_dir, f'daily_report_{sales_date}.csv')

    with open(report_file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Report Date', 'Total Sales', 'Total Quantity', 'Average Price', 'Number of Transactions', 'Unique Products Sold', 'Highest Selling Product ID', 'Highest Selling Product Name'])
        writer.writerows(report_data)

    logging.info(f'Daily sales report dated {sales_date} generated successfully at {report_file_path}')
    ti.xcom_push(key='report_file_path', value=report_file_path)

    return report_file_path

with DAG(
    dag_id='daily_sales_report',
    schedule_interval='@daily',
    start_date=datetime(2023, 4, 1),
    catchup=False,
    default_args={
        'owner': 'airflow',
    },
    params={
        'sales_date': Param(
            '',
            type='string',
            description='Date of sales data to be processed'
        )
    },
    tags = ['sales', 'report'],
    description='Data Pipeline for Daily Sales Reports'
) as dag:
    with TaskGroup(group_id='init_db_and_extract_data') as init_db_and_extract_data:

        init_db_task = PostgresOperator(
            task_id='init_db_task',
            postgres_conn_id='postgres_localhost',
            sql='''
                CREATE TABLE IF NOT EXISTS sales_data (
                    sale_id SERIAL PRIMARY KEY,
                    product_id INTEGER,
                    product_name VARCHAR(100) NOT NULL,
                    quantity INTEGER NOT NULL,
                    price DECIMAL(10, 2) NOT NULL,
                    sale_date DATE
                );
                
                CREATE TABLE IF NOT EXISTS daily_sales_reports (
                    report_date DATE PRIMARY KEY,
                    total_sales DECIMAL(10, 2),
                    total_quantity INTEGER,
                    average_price NUMERIC,
                    number_of_transactions INTEGER,
                    unique_products_sold INTEGER,
                    highest_selling_product_id INTEGER,
                    highest_selling_product_name TEXT
                );
            ''',
            autocommit=True
        )

        extract_data_task = PostgresOperator(
            task_id='extract_data_task',
            postgres_conn_id='postgres_localhost',
            sql='''
                SELECT *
                FROM sales_data
                WHERE sale_date = '{{params.sales_date}}';
            '''
        )

    transform_and_load_data_task = PythonOperator(
        task_id='transform_and_load_data_task',
        python_callable=_transform_and_load_data_,
        provide_context=True
    )

    report_generation_task = PythonOperator(
        task_id='report_generation_task',
        python_callable=_generate_report_,
        provide_context=True
    )


init_db_and_extract_data >> transform_and_load_data_task >> report_generation_task
