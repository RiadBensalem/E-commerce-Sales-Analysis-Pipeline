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

    def df_to_rows(df):
        return df.values.tolist()

    def load():
        client = clickhouse_connect.get_client(host='192.168.0.56',port="18123")
        products=pd.read_csv('/opt/airflow/datasets/products-generated.csv')
        customers=pd.read_csv('/opt/airflow/datasets/customers-transformed.csv')
        baskets=pd.read_csv('/opt/airflow/datasets/basket_details.csv')

        products=products[['base_price', 'discount_percentage', 'quantity', 'manufacturer',
       'tax_amount', 'product_id', 'category', 'sku', 'taxless_price',
       'unit_discount_amount', 'min_price', 'discount_amount',
       'created_on', 'product_name', 'price', 'taxful_price',
       'base_unit_price']]
        products['created_on']=pd.to_datetime(products['created_on'])
        products['manufacturer']=products['manufacturer'].astype('str')
        products['sku']=products['sku'].astype('str')
        products['product_name']=products['product_name'].astype('str')
        products['category']=products['category'].astype('str')
        client.command("""CREATE TABLE IF NOT EXISTS products (base_price Decimal32(2), discount_percentage Decimal32(2), quantity UInt16, manufacturer LowCardinality(Nullable(String)),
       tax_amount Decimal32(2), product_id UInt32, category Enum('Men\\'s Clothing', 'Women\\'s Clothing','Women\\'s Shoes','Men\\'s Accessories', 'Men\\'s Shoes','Women\\'s Accessories'), sku String, taxless_price Decimal32(2),
       unit_discount_amount Decimal32(3), min_price Decimal32(2), discount_amount Decimal32(3),
       created_on DateTime64(3), product_name Nullable(String), price Decimal32(2), taxful_price Decimal32(2),
       base_unit_price Decimal32(2)) ENGINE MergeTree ORDER BY product_id""")

        client.insert('products', df_to_rows(products.loc[0:products.shape[0]]), column_names=['base_price', 'discount_percentage', 'quantity', 'manufacturer',
       'tax_amount', 'product_id', 'category', 'sku', 'taxless_price',
       'unit_discount_amount', 'min_price', 'discount_amount',
       'created_on', 'product_name', 'price', 'taxful_price',
       'base_unit_price'])

        baskets['basket_date']=pd.to_datetime(baskets['basket_date'])
        client.command("CREATE TABLE IF NOT EXISTS baskets (customer_id UInt32, product_id UInt32, basket_date Date, basket_count UInt8) ENGINE MergeTree ORDER BY (customer_id, product_id)")
        client.insert('baskets', df_to_rows(baskets.loc[0:baskets.shape[0]]), column_names=['customer_id', 'product_id', 'basket_date', 'basket_count'])

        customers['sex']=customers['sex'].astype('str')
        client.command("CREATE TABLE IF NOT EXISTS customers (customer_id UInt32, sex Enum('male' = 1, 'female' = 2,'UNKNOWN'= 3), customer_age UInt64, tenure UInt8) ENGINE MergeTree ORDER BY customer_id")
        client.insert('customers', df_to_rows(customers.loc[0:customers.shape[0]]), column_names=['customer_id', 'sex', 'customer_age','tenure'])
        
        result=client.command("show tables")
        assert result=="baskets\ncustomers\nproducts"

    load_task=PythonOperator(
        task_id='load',
        python_callable=load,
        dag=dag
    )

    extract_task >> transform_task >> load_task