import csv
import logging
import os
from simple_salesforce import Salesforce
from supabase import create_client
from flask import Flask, jsonify

def get_environment_variable(variable_name):
    return os.environ.get(variable_name) or logging.error(f"Missing environment variable: {variable_name}")

SALESFORCE_USERNAME = get_environment_variable('SALESFORCE_USERNAME')
SALESFORCE_PASSWORD = get_environment_variable('SALESFORCE_PASSWORD')
SALESFORCE_SECURITY_TOKEN = get_environment_variable('SALESFORCE_SECURITY_TOKEN')
SALESFORCE_EXTERNAL_ID_FIELD = get_environment_variable('SALESFORCE_EXTERNAL_ID_FIELD')
SALESFORCE_MAPPING_OBJECT_NAME = get_environment_variable('SALESFORCE_MAPPING_OBJECT_NAME')
SALESFORCE_SOURCE_FIELD = get_environment_variable('SALESFORCE_SOURCE_FIELD')
SALESFORCE_DESTINATION_FIELD = get_environment_variable('SALESFORCE_DESTINATION_FIELD')
SALESFORCE_BATCH_SIZE = get_environment_variable('SALESFORCE_BATCH_SIZE')

SUPABASE_TABLE_NAME = get_environment_variable('SUPABASE_TABLE_NAME')
SUPABASE_URL = get_environment_variable('SUPABASE_URL')
SUPABASE_KEY = get_environment_variable('SUPABASE_KEY')

app = Flask(__name__)

@app.route('/api/action', methods=['GET'])
def receive_salesforce_request():
    try:
        upsert_devices_from_discovery_provider()
        return jsonify(status="success", message="Devices successfully updated."), 200
    except Exception as e:
        return jsonify(status="error", message=str(e)), 500

def connect_to_supabase():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def get_data_from_supabase(supabase):
    return supabase.table(SUPABASE_TABLE_NAME).select("*").execute().data

def connect_to_salesforce():
    return Salesforce(username=SALESFORCE_USERNAME, password=SALESFORCE_PASSWORD, security_token=SALESFORCE_SECURITY_TOKEN)

def get_attribute_mapping_from_salesforce(salesforce):
    return salesforce.query_all(f'SELECT Id, {SALESFORCE_SOURCE_FIELD}, {SALESFORCE_DESTINATION_FIELD} FROM {SALESFORCE_MAPPING_OBJECT_NAME}')

def create_attribute_mapping(response):
    return {record.get(SALESFORCE_SOURCE_FIELD): record.get(SALESFORCE_DESTINATION_FIELD)
        for record in response.get('records', [])
        if record.get(SALESFORCE_SOURCE_FIELD) and record.get(SALESFORCE_DESTINATION_FIELD)}

def get_attribute_mapping(salesforce):
    return create_attribute_mapping(get_attribute_mapping_from_salesforce(salesforce))

def apply_custom_mapping(data, attribute_mapping):
    return [
        {salesforce_field: item.get(discovery_provider_field, None)
        for discovery_provider_field, salesforce_field in attribute_mapping.items()}
        for item in data
    ]

def save_csv_file(data, filename='devices_data.csv'):
    if data:
        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        return filename
    return ""

def upsert_devices(filename, salesforce):
    try:
        return salesforce.bulk2.Configuration_Item__c.upsert(f"./{filename}", external_id_field=SALESFORCE_EXTERNAL_ID_FIELD, batch_size=int(SALESFORCE_BATCH_SIZE))
    finally:
        os.remove(filename) if os.path.exists(filename) else None

def upsert_devices_from_discovery_provider():
    try:
        logging.info("Connecting to Salesforce...")
        salesforce = connect_to_salesforce()

        logging.info("Getting attribute_mapping...")
        attribute_mapping = get_attribute_mapping(salesforce)

        logging.info("Connecting to Supabase...")
        supabase = connect_to_supabase()

        logging.info("Getting data from Supabase...")
        data = get_data_from_supabase(supabase)

        logging.info("Applying custom mapping...")
        mapped_data = apply_custom_mapping(data, attribute_mapping)

        logging.info("Saving CSV file...")
        filename = save_csv_file(mapped_data)

        logging.info("Upserting devices data...")
        result = upsert_devices(filename, salesforce)

        logging.info(result)
    except Exception as e:
        logging.error(f"Error in upsert_devices_from_discovery_provider: {e}", exc_info=True)
