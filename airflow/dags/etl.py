from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, date
import pandas as pd
from elasticsearch import Elasticsearch
import clickhouse_connect
from collections import OrderedDict
import random

with DAG(
    dag_id='e-commerce_analysis_etl',
    schedule_interval=None,
    catchup=False,
    start_date=datetime(2023,1,1)
    ) as dag:
    
    def extract():
        es = Elasticsearch(['https://192.168.0.56:9200'],
                            http_auth=('elastic', 'BlnQuVwkGQy09E=S+d*S'),
                            verify_certs=False
                        )

        # Specify the index name
        index_name = "kibana_sample_data_ecommerce" 
        # Define a query (optional)
        query = {
            "size":10000,
            "query": {
                "match_all": {}
            }
        }
        # Execute the search query
        result = es.search(index=index_name, body=query)
        products = OrderedDict()
        # Process and print the search results
        for hit in result['hits']['hits']:
            for product in hit['_source']["products"]:
                try:
                    products[product['product_id']]=product
                except:
                    continue
        prdcts=pd.DataFrame(products).T
        prdcts.to_csv("/opt/airflow/datasets/products.csv")

    extract_task=PythonOperator(
        task_id='extract',
        python_callable=extract,
        dag=dag
    )

    def generate_product(values):
        product={}
        for k,v in values.items():
            product[k]=random.choice(v)
        product['product_id']= -1 
        return product

    def transform():
        prdcts=pd.read_csv('/opt/airflow/datasets/products.csv')
        baskets=pd.read_csv("/opt/airflow/datasets/basket_details.csv")
        columns_values={}
        for c in prdcts.columns:
            if c!='product_id':
                columns_values[c]=prdcts[c].unique()
        generated={}
        number_products=len(baskets.product_id.unique())
        for i in range(number_products-prdcts.shape[0]):
            generated[i]=generate_product(columns_values)
        
        gennerated_df=pd.DataFrame(generated).T
        prdcts=pd.concat([prdcts,gennerated_df], ignore_index=True)
        prdcts.to_csv("/opt/airflow/datasets/products-generated.csv")

    transform_task=PythonOperator(
        task_id="transform",
        python_callable=transform,
        dag=dag
    )

    def load():
        return 3

    load_task=PythonOperator(
        task_id='load',
        python_callable=load,
        dag=dag
    )

    extract_task >> transform_task >> load_task