# Airflow
```
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.6.3/docker-compose.yaml'
```

```
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker compose up airflow-init
docker compose up

```

# ClickHouse

```
docker pull clickhouse/clickhouse-server
```

```
docker run -d -p 18123:8123 -p 19000:9000 -v /home/oem/Documents/Learning/E-commerce-Sales-Analysis-Pipeline/clickhouse/data:/var/lib/clickhouse/ -v /home/oem/Documents/Learning/E-commerce-Sales-Analysis-Pipeline/clickhouse/logs:/var/log/clickhouse-server/ --name e-commerce-analysis-clickhouse-server --ulimit nofile=262144:262144 clickhouse/clickhouse-server
```

# Grafana
```
docker run -d -p 3000:3000 --name=grafana -e "GF_INSTALL_PLUGINS=grafana-clock-panel, grafana-simple-json-datasource, grafana-worldmap-panel, grafana-clickhouse-datasource" --user "$(id -u)"   --volume "/home/oem/Documents/Learning/E-commerce-Sales-Analysis-Pipeline/grafana/data:/var/lib/grafana"   grafana/grafana-enterprise
```
