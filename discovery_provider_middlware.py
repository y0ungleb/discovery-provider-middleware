import csv
import json
import traceback
from simple_salesforce import Salesforce
from supabase import create_client, Client
from flask import Flask, request, jsonify

app = Flask(__name__)

SALESFORCE_USERNAME = 'gleb.goose@cunning-impala-v6bgqi.com'
SALESFORCE_PASSWORD = 'CunningImpala123'
SALESFORCE_SECURITY_TOKEN = '3qbRDV3mTpPPqQXD4mWea3km'

SUPABASE_URL='https://potljjxcvdgdzvjcctub.supabase.co'
SUPABASE_KEY='eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBvdGxqanhjdmRnZHp2amNjdHViIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDA0NzI2MjksImV4cCI6MjA1NjA0ODYyOX0.ENERcdaG6zRwU-F1CqqZX9nGH1LIWKX3R-nSQjX2uUI'

@app.route('/api/action', methods=['GET'])
def receive_salesforce_request():
    try:
        print("Received GET request from Salesforce:")


        return jsonify({
            "status": "success",
            "message": "GET request received"
        }), 200
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

def connect_to_supabase():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def get_data_from_supabase(supabase):
    return supabase.table("Endpoints").select("*").execute()

def connect_to_salesforce():
    return Salesforce(username=SALESFORCE_USERNAME, password=SALESFORCE_PASSWORD, security_token=SALESFORCE_SECURITY_TOKEN)

def get_attribute_mapping_from_salesforce(salesforce):
    return salesforce.query_all('SELECT Id, Source_Field__c, Destination_Field__c FROM Attribute_Mapping__mdt')

def create_attribute_mapping_from_response(response):
    attribute_mapping = {}

    if 'records' in response:
        for record in response['records']:
            source_field = record.get('Source_Field__c')
            destination_field = record.get('Destination_Field__c')

            if source_field and destination_field:
                attribute_mapping[source_field] = destination_field

    return attribute_mapping

def get_attribute_mapping(salesforce):
    response = get_attribute_mapping_from_salesforce(salesforce)
    return create_attribute_mapping_from_response(response)

def apply_custom_mapping(data, attribute_mapping):
    mapped_data = []
    for item in data:
        mapped_item = {}
        for tanium_field, salesforce_field in attribute_mapping.items():
            if tanium_field in item:
                mapped_item[salesforce_field] = item[tanium_field]
            else:
                mapped_item[salesforce_field] = None
        mapped_data.append(mapped_item)
    return mapped_data

def save_csv_file(data, filename='devices_data.csv'):
    if not data:
        return ""

    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

    return filename

def insert_devices(filename, salesforce):
    result = salesforce.bulk2.Configuration_Item__c.insert(f"./{filename}", batch_size=10000)
    print(result)

def upsert_devices(filename, salesforce):
    result = salesforce.bulk2.Configuration_Item__c.upsert(f"./{filename}", external_id_field="External_Id__c", batch_size=10000)
    print(result)

def upsert_devices_from_discovery_provider():
    try:
        salesforce = connect_to_salesforce()
        attribute_mapping = get_attribute_mapping(salesforce)

        supabase = connect_to_supabase()
        data = get_data_from_supabase(supabase)

        mapped_data = apply_custom_mapping(json.loads(data.json())['data'], attribute_mapping)
        filename = save_csv_file(mapped_data)

        upsert_devices(filename, salesforce)
    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()

if __name__ == '__main__':
    print("Waiting for requests...")
    app.run(host='0.0.0.0', port=5000, debug=True)

# TODO deploy to Heroku
# TODO create environment variables for auth
# TODO add saving to Heroku database
