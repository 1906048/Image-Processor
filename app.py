from flask import Flask, request, jsonify
import csv
import uuid
import os
from pymongo import MongoClient
from celery import Celery
from werkzeug.utils import secure_filename
from bson.objectid import ObjectId

app = Flask(__name__)

# MongoDB setup
client = MongoClient("mongodb://localhost:27017/")
db = client['image_processing_db']
requests_collection = db['requests']

# Celery configuration
app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'mongodb://localhost:27017/image_processing_db'

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

# Upload API
@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    filename = secure_filename(file.filename)
    file_path = os.path.join("/tmp", filename)
    file.save(file_path)
    
    # Generate unique request ID
    request_id = str(uuid.uuid4())
    
    # Store initial request details in MongoDB
    request_data = {
        "request_id": request_id,
        "status": "Pending",
        "input_file": filename,
        "output_file": None,
        "input_data": [],
        "output_data": [],
        "webhook_url": request.form.get('webhook_url')
    }
    requests_collection.insert_one(request_data)
    
    # Start asynchronous task to process the CSV
    process_csv_file.delay(request_id, file_path)
    
    return jsonify({"request_id": request_id}), 202

# Status API
@app.route('/status/<request_id>', methods=['GET'])
def get_status(request_id):
    result = requests_collection.find_one({"request_id": request_id})
    
    if result:
        return jsonify({"request_id": request_id, "status": result['status'], "output_data": result.get('output_data', [])})
    return jsonify({"error": "Invalid request ID"}), 404

if __name__ == '__main__':
    app.run(debug=True)
