FROM apache/airflow:2.7.2

COPY requirements.txt ./
COPY .env ./

RUN pip install --upgrade pip && \
    pip install -r requirements.txt \

ENTRYPOINT ["airflow"]