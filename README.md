# bonbanh_crawler
## Set up Airflow
### Setting the right Airflow user
Run this command:
```shell
mkdir airflow
cd airflow
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
and complete ```.env``` file...

### Generate Fernet Key and Secret Key
```shell
# This for AIRFLOW__CORE__FERNET_KEY
from cryptography.fernet import Fernet
fernet_key = Fernet.generate_key()
print(fernet_key.decode())
# This for AIRFLOW__WEBSERVER__SECRET_KEY
import os
print(os.urandom(16))
```
Add your generated Fernet Key and Secret Key to your .env file
### Build Airflow
```shell
docker compose -f master.yaml build
```
Setup airflow database using airflow-init, and deploy airflow webserver, scheduler, worker, triggerer, and flower to master node:
```shell
docker compose -f master.yaml --profile flower up -d
```
## Steps:
1. Setup environment (`.env`) of airflow-cluster, projects
2. Setup fernet key and secret key
3. Run Airflow
4. Config Airflow variable to set up dag
5. Run dag

## TODO
### Install chrome and/or chromedriver
Update Dockerfile by adding:
```shell
RUN apt-get update
RUN apt-get install -y wget
RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN apt-get install -y ./google-chrome-stable_current_amd64.deb
```
OR execute every worker Docker by command:
```shell
docker exec -u 0 -it woker1 /bin/bash
```
and run commands 