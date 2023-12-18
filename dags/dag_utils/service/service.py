from datetime import datetime


def reformat(values):
    try:
        values = values.replace("\n", "")
        values = values.replace("\t", "")
    except:
        pass

    return values


def convert_price(input_string):
    # Chia chuỗi thành các từ riêng lẻ
    words = input_string.strip().split()

    # Tìm và chuyển đổi các giá trị "Tỷ" và "Triệu"
    billion_idx = words.index("Tỷ") if "Tỷ" in words else -1
    million_idx = words.index("Triệu") if "Triệu" in words else -1

    billion_value = float(words[billion_idx - 1]) if billion_idx > 0 else 0
    million_value = float(words[million_idx - 1]) if million_idx > 0 else 0

    # Tính tổng giá trị
    total_value = billion_value + (million_value / 1000)

    return total_value


def tranform_to_int(str):
    try:
        return int(str)
    except:
        return str


def get_old_link():
    old_links = []
    with open('/opt/airflow/dags/dag_utils/service/link.txt', "r") as file:
        for line in file:
            link = line.replace("\n", "")
            old_links.append(str(link))

    return old_links


def write_page_source(page_source):
    with open('/opt/airflow/dags/dag_utils/service/page_source.txt', "w") as file:
        file.write(page_source)


def write_first_line(links):
    with open('/opt/airflow/dags/dag_utils/service/link.txt', "a") as file:
        for link in links:
            link = link + "\n"
            file.write(link)


def get_file_name():
    # Lấy ngày tháng năm hiện tại
    current_date = datetime.now()

    # Chuyển định dạng ngày tháng năm thành DDMMYYYY
    formatted_date = current_date.strftime("%d%m%Y")
    json_file_name = "data" + str(formatted_date) + ".json"
    return json_file_name
