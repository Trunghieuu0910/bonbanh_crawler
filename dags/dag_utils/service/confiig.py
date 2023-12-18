import yaml

with open('/opt/airflow/dags/dag_utils/service/confiig.yml') as config_file:
    config = yaml.load(config_file, Loader=yaml.FullLoader)