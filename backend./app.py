from flask import Flask, request, jsonify
from flask_cors import CORS
import boto3
import json
import logging
import os
import decimal
import pymysql
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from contextlib import contextmanager

# Load environment variables
load_dotenv()

app = Flask(__name__)
CORS(app)

# Logging Configuration
logging.basicConfig(level=logging.DEBUG)

# AWS S3 Configuration
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
S3_BUCKET = os.getenv('S3_BUCKET')
AWS_REGION = os.getenv('AWS_REGION', 'ap-southeast-2')

# Azure Blob Storage Configuration

AZURE_STORAGE_CONNECTION_STRING = ''
AZURE_CONTAINER_NAME = ''

# MySQL Database Configuration
MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', '')
MYSQL_DB = os.getenv('MYSQL_DB', 'transfer')

# Initialize AWS S3 client
try:
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )
    logging.info("AWS S3 client initialized successfully.")
except Exception as e:
    logging.error(f"Error initializing AWS S3 client: {str(e)}")
    s3_client = None

# Initialize Azure Blob Storage client
try:
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    logging.info("Azure Blob Storage client initialized successfully.")
except Exception as e:
    logging.error(f"Error initializing Azure Blob Storage client: {str(e)}")
    blob_service_client = None

# Define decimal_default function
def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError

# Database connection context manager
@contextmanager
def db_connection():
    connection = None
    try:
        connection = pymysql.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB
        )
        logging.info("Connected to MySQL database successfully.")
        yield connection
    except Exception as e:
        logging.error(f"Error connecting to MySQL: {str(e)}")
        raise e
    finally:
        if connection:
            connection.close()

# Create MySQL tables
def create_tables():
    try:
        with db_connection() as connection:
            with connection.cursor() as cursor:
                # Create policy table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS policy (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        policy_id VARCHAR(50) NOT NULL,
                        policy_type VARCHAR(255) NOT NULL,
                        base_premium DECIMAL(10, 2) NOT NULL,
                        vehicle_damage DECIMAL(10, 2) NOT NULL,
                        risk_factor VARCHAR(20) NOT NULL,
                        discount DECIMAL(5, 2) NOT NULL,
                        branch_id VARCHAR(10) NOT NULL
                    )
                ''')

                # Create customer_info table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS customer_info (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        policy_id VARCHAR(50) NOT NULL,
                        name VARCHAR(255) NOT NULL,
                        age INT NOT NULL,
                        address VARCHAR(255) NOT NULL
                    )
                ''')

                # Create vehicle_info table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS vehicle_info (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        policy_id VARCHAR(50) NOT NULL,
                        make VARCHAR(255) NOT NULL,
                        model VARCHAR(255) NOT NULL,
                        year INT NOT NULL,
                        vehicle_damage DECIMAL(10, 2) NOT NULL
                    )
                ''')

                # Create coverage_info table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS coverage_info (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        policy_id VARCHAR(50) NOT NULL,
                        liability DECIMAL(10, 2) NOT NULL,
                        collision DECIMAL(10, 2) NOT NULL,
                        comprehensive DECIMAL(10, 2) NOT NULL,
                        discount DECIMAL(5, 2) NOT NULL
                    )
                ''')

            connection.commit()
            logging.info("MySQL tables created successfully.")
    except Exception as e:
        logging.error(f"Error creating tables: {str(e)}")

# Initialize MySQL tables
create_tables()

# Endpoint for Azure to request a file from S3
@app.route('/fetch-from-s3', methods=['POST'])
def fetch_from_s3():
    try:
        # Step 1: Get the file key (name) from Azure's request
        data = request.get_json()
        if not data or 'file_key' not in data:
            return jsonify({"error": "Missing 'file_key' in request"}), 400

        file_key = data['file_key']

        # Step 2: Fetch the file from AWS S3
        if not s3_client:
            return jsonify({"error": "AWS S3 client not initialized"}), 500

        try:
            s3_object = s3_client.get_object(Bucket=S3_BUCKET, Key=file_key)
            file_data = s3_object['Body'].read()
            logging.info(f"File '{file_key}' fetched from AWS S3 successfully.")
        except Exception as e:
            logging.error(f"Error fetching file from AWS S3: {str(e)}")
            return jsonify({"error": f"Error fetching file from AWS S3: {str(e)}"}), 500

        # Step 3: Upload the file to Azure Blob Storage
        if not blob_service_client:
            return jsonify({"error": "Azure Blob Storage client not initialized"}), 500

        try:
            blob_client = blob_service_client.get_blob_client(container=AZURE_CONTAINER_NAME, blob=file_key)
            blob_client.upload_blob(file_data, overwrite=True)
            logging.info(f"File '{file_key}' uploaded to Azure Blob Storage successfully.")
        except Exception as e:
            logging.error(f"Error uploading file to Azure Blob Storage: {str(e)}")
            return jsonify({"error": f"Error uploading file to Azure Blob Storage: {str(e)}"}), 500

        return jsonify({"message": f"File '{file_key}' migrated from AWS S3 to Azure Blob Storage successfully!"}), 200

    except Exception as e:
        logging.error(f"Error in /fetch-from-s3: {str(e)}")
        return jsonify({"error": str(e)}), 500

# Endpoint for processing and uploading data
@app.route('/process-and-upload', methods=['POST'])
def upload_and_process():
    try:
        # Step 1: Read JSON data from request
        if 'file' in request.files:
            file = request.files['file']
            json_data = file.read().decode('utf-8')
        elif 'jsonData' in request.form:
            json_data = request.form['jsonData']
        else:
            return jsonify({"error": "No data provided"}), 400

        try:
            data = json.loads(json_data)
        except json.JSONDecodeError as e:
            return jsonify({"error": f"Invalid JSON: {str(e)}"}), 400

        # Step 2: Database Connection and Insertion
        with db_connection() as connection:
            with connection.cursor() as cursor:
                for branch in data['branches']:
                    branch_id = branch.get('branch_id')

                    for policy in branch['policies']:
                        # Insert into policy table
                        cursor.execute('''
                            INSERT INTO policy (
                                policy_id, policy_type, base_premium,
                                vehicle_damage, risk_factor, discount, branch_id
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ''', (
                            policy['policy_id'], policy['policy_type'],
                            policy['base_premium'], policy['vehicle_info']['vehicle_damage'],
                            policy['risk_factor'], policy['coverage_info']['discount'],
                            branch_id
                        ))

                        # Insert into customer_info table
                        cursor.execute('''
                            INSERT INTO customer_info (
                                policy_id, name, age, address
                            ) VALUES (%s, %s, %s, %s)
                        ''', (
                            policy['policy_id'],
                            policy['customer_info']['name'],
                            policy['customer_info']['age'],
                            policy['customer_info']['address']
                        ))

                        # Insert into vehicle_info table
                        cursor.execute('''
                            INSERT INTO vehicle_info (
                                policy_id, make, model, year, vehicle_damage
                            ) VALUES (%s, %s, %s, %s, %s)
                        ''', (
                            policy['policy_id'],
                            policy['vehicle_info']['make'],
                            policy['vehicle_info']['model'],
                            policy['vehicle_info']['year'],
                            policy['vehicle_info']['vehicle_damage']
                        ))

                        # Insert into coverage_info table
                        cursor.execute('''
                            INSERT INTO coverage_info (
                                policy_id, liability, collision, comprehensive, discount
                            ) VALUES (%s, %s, %s, %s, %s)
                        ''', (
                            policy['policy_id'],
                            policy['coverage_info']['liability'],
                            policy['coverage_info']['collision'],
                            policy['coverage_info']['comprehensive'],
                            policy['coverage_info']['discount']
                        ))

                connection.commit()

        # Step 3: Fetch and Process Data
        with db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute('''
                    SELECT 
                        policy_id,
                        policy_type,
                        base_premium,
                        vehicle_damage,
                        risk_factor,
                        discount,
                        base_premium 
                        + (vehicle_damage * 
                           CASE risk_factor
                               WHEN 'low' THEN 1.0
                               WHEN 'medium' THEN 1.5
                               WHEN 'high' THEN 2.0
                               ELSE 1.0
                           END)
                        - (base_premium * (discount / 100)) AS calculated_premium,
                        CASE
                            WHEN (base_premium 
                                  + (vehicle_damage * 
                                     CASE risk_factor
                                         WHEN 'low' THEN 1.0
                                         WHEN 'medium' THEN 1.5
                                         WHEN 'high' THEN 2.0
                                         ELSE 1.0
                                     END)
                                  - (base_premium * (discount / 100))) < 10000
                            AND risk_factor IN ('low', 'medium')
                            THEN 'Granted'
                            ELSE 'Rejected'
                        END AS insurance_granted
                    FROM policy
                ''')

                processed_data = cursor.fetchall()

        # Step 4: Prepare JSON data for AWS S3
        policies = []
        for row in processed_data:
            policies.append({
                "policy_id": row[0],
                "policy_type": row[1],
                "base_premium": float(row[2]),
                "vehicle_damage": float(row[3]),
                "risk_factor": row[4],
                "discount": float(row[5]),
                "calculated_premium": float(row[6]),
                "insurance_granted": row[7]
            })

        json_data = json.dumps(policies, indent=2, default=decimal_default)

        # Step 5: Upload Processed Data to AWS S3
        if s3_client:
            try:
                response = s3_client.put_object(
                    Bucket=S3_BUCKET,
                    Key='insurance_data.json',
                    Body=json_data
                )
                logging.info(f"Step 5: S3 Upload Successful. Response: {response}")
            except Exception as s3_error:
                logging.error(f"Error uploading to AWS S3: {str(s3_error)}")
                return jsonify({"error": f"Error uploading data to AWS S3: {str(s3_error)}"}), 500
        else:
            logging.error("AWS S3 client not initialized.")
            return jsonify({"error": "AWS S3 client not initialized."}), 500

        return jsonify({
            "message": "Data successfully stored in MySQL and then uploaded to AWS S3!"
        }), 200

    except Exception as e:
        logging.error(f"Error in /upload-and-process: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5001)
