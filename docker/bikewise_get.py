import requests
import json
import configparser
import datetime
import os
import psycopg2

# store data to s3
import boto3
from botocore.exceptions import ClientError

# various of settings
config = configparser.ConfigParser()
config.read('setting.conf')

os.environ["AWS_ACCESS_KEY_ID"] =  config['DEFAULT']["AWS_ACCESS_KEY"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config['DEFAULT']["AWS_SECRET_KEY"]
os.environ["AWS_DEFAULT_REGION"] = config['DEFAULT']["AWS_DEFAULT_REGION"]
warehouse_host = config['DEFAULT']["WAREHOUSE_HOST"]
warehouse_account = config['DEFAULT']["WHAREHOUSE_ACCOUNT"]
warehouse_password = config['DEFAULT']["WAREHOUSE_PASSWORD"]


the_date = datetime.datetime.today()

def get_request():
    h_url = "https://bikeindex.org:443/api/v3/search?page="
    t_url = "&per_page=100&location=IP&distance=10&stolenness=all"
    stamp = 0
    for page in range(1, 4):
        f_url = h_url + str(page) + t_url
        print(f_url)
        
        r = requests.get(f_url).json()
        bike_data = r['bikes']
        print(len(bike_data))
        print(f"writing page {page} into json file")


        file_name = f'./temp/output_{stamp}.jsonl'
        with open(file_name, 'w') as outfile:
            for entry in bike_data:
                json.dump(entry, outfile)
                outfile.write('\n')
        stamp += 1
    


def upload_to_s3(date):
    '''
    1. write data onto s3 (run with schedule)
    2. partition by year, month, day
    3. so team member only need to use sql query on athena
    4. To use AWS lake formation to have authentication on the data

    bikewise
        data
            2020
                07
                    14
                        xxx.json
                    15
                        xxx.json
                    16
                        xxx.json
        test
            2020
                07
                   14
                        xxx.json
                   15
                        xxx.json
                   16
                        xxx.json
    '''
    s3_client = boto3.client('s3')
    year =  date.year
    month = date.month
    day = date.day
    bucket_name = config["DEFAULT"]["S3BUCKET_NAME"]
    key_name = f"test/{year}/{month}/{day}/"

    for file in os.listdir("./temp"):
        try:
            s3_file_path = os.path.join(key_name, file)
            local_file = os.path.join(os.getcwd(), "temp", file)
            response = s3_client.upload_file(local_file, bucket_name, s3_file_path)
            print(f"{local_file} upload to {bucket_name} , path: {s3_file_path}")
        except s3_client.ClientError as e:
            print(e)



def write_to_warehouse(warehouse_host, warehouse_account, warehouse_password):
    # Laod daily day into memory 
    cache_data = list()
    for file in os.listdir("./temp"):
        local_file = os.path.join(os.getcwd(), "temp", file)
        print(f"Reading {local_file}")
        with open(local_file, 'r') as read_file:
            file_lines = read_file.readlines()
            for line in file_lines:
                data = json.loads(line)
                # formating the row data
                row_data = []
                row_data.append(data['date_stolen'])
                row_data.append(data['description'])
                row_data.append(data['frame_colors'])
                row_data.append(data['frame_model'])
                row_data.append(data['id'])
                row_data.append(data['is_stock_img'])
                row_data.append(data['large_img'])
                row_data.append(data['location_found'])
                row_data.append(data['manufacturer_name'])
                row_data.append(data['external_id'])
                row_data.append(data['registry_name'])
                row_data.append(data['registry_url'])
                row_data.append(data['serial'])
                row_data.append(data['status'])
                row_data.append(data['stolen'])
                row_data.append(data['stolen_location'])
                row_data.append(data['thumb'])
                row_data.append(data['title'])
                row_data.append(data['url'])
                row_data.append(data['year'])

                cache_data.append(tuple(row_data))

    # Write memory into stage table
    login_info =f"dbname=postgres user={warehouse_account} password={warehouse_password} host={warehouse_host}"
    with psycopg2.connect(login_info) as conn:
        cur = conn.cursor()
        print("Start to load data into stage table...")
        sql_query = """
            INSERT INTO bikewise_stage VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s )
        """
        cur.executemany(sql_query, cache_data)

def clean_up():
    for file in os.listdir("./temp"):
        local_file = os.path.join(os.getcwd(), "temp", file)
        print(f"file {local_file} cleaning up...")
        os.remove(local_file)

    
if __name__ == "__main__":
    get_request()
    upload_to_s3(the_date)
    write_to_warehouse(warehouse_host, warehouse_account, warehouse_password)
    clean_up()



