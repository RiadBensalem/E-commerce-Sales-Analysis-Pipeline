from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.exceptions import AirflowFailException
from datetime import datetime, date
import pandas as pd
from elasticsearch import Elasticsearch
import clickhouse_connect
from collections import OrderedDict
import random
import traceback
import sys

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
        prdcts.to_csv("/opt/airflow/datasets/products.csv",index=False)

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
        customers=pd.read_csv("/opt/airflow/datasets/customer_details.csv")

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
        prdcts.product_id=baskets.product_id.unique()
        prdcts['created_on']=prdcts['created_on'].map(lambda x: x.replace("T",' ')[:-6])
        prdcts.to_csv("/opt/airflow/datasets/products-generated.csv",index=False)

        customers["sex"]=customers["sex"].map(lambda x : "UKNOWN" if ((x.lower().strip()!="female") and (x.lower().strip()!='male')) else x.lower().strip() )
        customers["customer_age"]=customers["customer_age"].map(lambda x : None if (x > 100) else int(abs(x)))
        customers["customer_age"]=customers["customer_age"].fillna(customers["customer_age"].median())
        customers["customer_age"]=customers["customer_age"].astype(int)
        try:
            assert customers["tenure"].isna().sum()==0
            assert customers["customer_age"].isna().sum()==0
            assert customers["sex"].isna().sum()==0
            assert customers["customer_id"].isna().sum()==0
            assert all(customers["sex"].unique()== ['male', 'female', 'UKNOWN'])
            assert all(customers["customer_age"].unique()>0) and all(customers["customer_age"].unique()< 101)

            customers.to_csv("/opt/airflow/datasets/customers-transformed.csv",index=False)

            assert baskets["customer_id"].isna().sum()==0
            assert baskets["product_id"].isna().sum()==0
            assert baskets["basket_date"].isna().sum()==0
            assert baskets["basket_count"].isna().sum()==0
            assert all(baskets["basket_count"].unique()>0)

        except AssertionError:
            _, _, tb = sys.exc_info()
            traceback.print_tb(tb)
            tb_info = traceback.extract_tb(tb)
            filename, line, func, text = tb_info[-1]
            raise AirflowFailException('An error occurred on line {} in statement {}'.format(line, text))


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