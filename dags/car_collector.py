import time

import json

from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from dag_utils.generator import generator
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import datetime
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from dag_utils.service.service import reformat, convert_price, tranform_to_int, get_old_link, get_file_name, \
    write_first_line
from dag_utils.service.confiig import config


@generator
def car_collector(dag_id, schedule="@daily", **kwargs):
    dag = DAG(
        dag_id=dag_id,
        schedule=schedule, start_date=datetime.datetime(2023, 5, 15),
        catchup=False,
        max_active_runs=2,
        tags=['Graph Loader', 'Offchain Collector'],
        dagrun_timeout=datetime.timedelta(minutes=60)
    )

    with dag:
        @task
        def crawl_page():
            old_link = get_old_link()
            print(old_link)
            new_link = []
            driver = get_driver()
            for current_page in range(1, 2):
                driver.get('http://bonbanh.com/' + f"oto/page,{current_page}")
                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')
                product_details_url_containers = soup.select(".row1") + soup.select(".row2")
                if product_details_url_containers:
                    for product_details_url_container in product_details_url_containers:
                        tag = product_details_url_container.find('a', itemprop='url')
                        link = tag.get('href')
                        link = "https://bonbanh.com/" + link
                        if link not in old_link:
                            new_link.append(link)
                else:
                    print("Don't have link to crawl")

            print(len(new_link))
            write_first_line(new_link)
            return new_link

        @task
        def crawl_item(links):
            driver = get_driver()
            list_items = []
            for link in links:
                driver.get(link)
                page_source = driver.page_source
                if "Vui Lòng Tick vào ô" in driver.page_source:
                    while "Vui Lòng Tick vào ô" in page_source:
                        page_source = driver.page_source
                        time.sleep(1)
                        print(time.time())
                else:
                    page_source = driver.page_source
                    soup = BeautifulSoup(page_source, 'html.parser')
                    try:
                        info_containers = soup.select(config.get("info_containers_css"))
                        title = reformat(soup.select_one("h1").get_text())
                        author = reformat(soup.select_one(".cname").get_text())
                        description = reformat(soup.select_one(".des_txt").get_text())
                        car_name = reformat(soup.select_one("span:nth-child(4) strong").get_text())
                        car_company = reformat(soup.select_one("span:nth-child(3) strong").get_text())
                        year_man = reformat(soup.select_one("span:nth-child(5) strong").get_text())
                        img_tags = soup.select('div.highslide-gallery img')
                        img_link = [img_tag['src'] for img_tag in img_tags]
                        phone_number = reformat(soup.select_one(".cphone").get_text())
                        _date = reformat(soup.select_one(".notes").get_text())
                        try:
                            _date = _date[10:_date.find('.')]
                        except:
                            _date = datetime.date.today()
                        price = title[title.rfind('-') + 2:]
                        price = convert_price(price)
                        engine = reformat(soup.select_one(".col_l+ .col #mail_parent:nth-child(1) .inp").get_text())
                        info_elements = soup.find_all("span", class_="inp")

                        car_info = {}

                        for element in info_elements:
                            # Lấy tên trường thông tin (năm sản xuất, tình trạng, xuất xứ)
                            label = element.find_previous("label").text.strip()

                            value = element.get_text().strip()

                            car_info[label] = value

                        item = {}
                        item['title'] = title
                        item['author'] = author
                        item['engine'] = engine
                        item['type'] = reformat(car_info.get("Kiểu dáng:"))
                        try:
                            item['number_Km'] = tranform_to_int(
                                reformat(car_info.get("Số Km đã đi:")).replace(',', '').replace('Km', ''))
                        except:
                            item['number_Km'] = 0
                        item['gear'] = reformat(car_info.get("Hộp số:"))
                        item['status'] = reformat(car_info.get("Tình trạng:"))
                        item['origin'] = reformat(car_info.get("Xuất xứ:"))
                        try:
                            item['number_seats'] = tranform_to_int(
                                reformat(car_info.get("Số chỗ ngồi:")).replace(' chỗ', ''))
                        except:
                            item['number_seats'] = 0
                        try:
                            item['number_door'] = tranform_to_int(reformat(car_info.get("Số cửa:")).replace(' cửa', ''))
                        except:
                            item['number_door'] = 0
                        item['price'] = price
                        item['exterior_color'] = reformat(car_info.get("Màu ngoại thất:"))
                        item['interior_color'] = reformat(car_info.get("Màu nội thất:"))
                        item['fuel_consumption'] = reformat(car_info.get("Tiêu thụ nhiên liệu:"))
                        item['description'] = description
                        item['car_name'] = car_name
                        item['car_company'] = car_company
                        item['year_man'] = year_man
                        item['link'] = link
                        item['img_link'] = img_link
                        item['date'] = _date
                        item['phone_number'] = phone_number
                        list_items.append(item)

                    except Exception as e:
                        print(e)

            if len(list_items) > 0:
                save_dictionary(list_items)

        @task
        def tranform():
            from minio import Minio
            json_file_name = get_file_name()
            access_key = '6YukH96BjbdO2JwieS2A'
            secret_key = 'Jb4ifyQwO0R8ftY20h4TKakASSYKhTv1iKLK8SM4'

            # Tạo đối tượng Minio
            minio_client = Minio('20.243.203.105:9000',
                                 access_key=access_key,
                                 secret_key=secret_key,
                                 secure=False)

            response = minio_client.get_object('hust', 'car/' + json_file_name)
            json_data = json.loads(response.data.decode('utf-8'))

            #TODO: transfrom data

            return json_data

        @task
        def load(data):
            from pymongo import MongoClient
            mongo_uri = "mongodb://20.243.203.105:27017/?directConnection=true&appName=mongosh+2.1.1"

            # Tạo kết nối đến MongoDB
            client = MongoClient(mongo_uri)

            # Chọn cơ sở dữ liệu (db) và bảng (collection) cụ thể
            db = client['hust']
            collection = db['car']

            result = collection.insert_many(data)

            # In ID của các bản ghi vừa được chèn
            print(f"ID của các bản ghi vừa được chèn: {result.inserted_ids}")
            client.close()

        with TaskGroup("Extract") as extract:
            new_link = crawl_page()
            crawl_item(new_link)

        with TaskGroup("Transform_And_Load") as group:
            json_data = tranform()
            load(data=json_data)

        extract >> group

car_collector()


def get_driver():
    # chrome_options = Options()
    # # chrome_options.binary_location = '/usr/local/bin/chromedriver'
    # # chrome_options.add_argument('--headless')
    # chrome_options.add_argument(
    #     'user-agent=Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Mobile Safari/537.36')
    # driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options))

    options = Options()
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-setuid-sandbox")
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--headless')
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
    return driver


def save_dictionary(list_of_dicts):
    import os
    from minio import Minio
    import json

    json_file_name = get_file_name()
    json_file_path = '/opt/airflow/dags/dag_utils/data/' + json_file_name

    # Ghi danh sách dictionaries vào tệp tin JSON
    with open(json_file_path, 'w') as json_file:
        json.dump(list_of_dicts, json_file, indent=2)

    # Thông tin kết nối đến MinIO
    access_key = '6YukH96BjbdO2JwieS2A'
    secret_key = 'Jb4ifyQwO0R8ftY20h4TKakASSYKhTv1iKLK8SM4'

    # Tạo đối tượng Minio
    minio_client = Minio('20.243.203.105:9000',
                         access_key=access_key,
                         secret_key=secret_key,
                         secure=False)
    try:
        minio_client.fput_object('hust', 'car/' + json_file_name, json_file_path)
        os.remove(json_file_path)
    except Exception as e:
        print(e)
