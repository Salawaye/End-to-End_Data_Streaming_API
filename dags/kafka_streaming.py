from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'Ibrahim Alawaye'
    #'strart_date': datetime(2024, 4, 10, 12, 00)
}


def get_data():
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]
    
    return res

def format_data(res_func):
    # Call the function to get the dictionary
    res = res_func()

    # Decoding Unicode escape sequences in the address
    address = res['location']['street']['name']
    decoded_address = address.encode().decode('unicode_escape')

    # Replace the address in the original dictionary
    res['location']['street']['name'] = decoded_address

    # Proceed with formatting as before
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {decoded_address} " \
        f"{location['city'] + ', ' + location['state']} {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data


                           

def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    res = get_data
    res = format_data(res)
    # print(json.dumps(res, indent=3))

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)

    producer.send('users_created', json.dumps(res).encode('utf-8'))
    
 



stream_data();