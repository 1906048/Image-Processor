from celery import Celery
from pymongo import MongoClient
import csv
import requests
from image_processor import compress_image
from gridfs import GridFS
import os

# MongoDB connection
client = MongoClient("mongodb://localhost:27017/")
db = client['image_processing_db']
requests_collection = db['requests']
fs = GridFS(db)

celery = Celery('tasks', broker='redis://localhost:6379/0')

@celery.task
def process_csv_file(request_id, file_path):
    request_data = requests_collection.find_one({"request_id": request_id})
    
    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        input_data = []
        output_data = []

        for row in reader:
            serial_number = row[0]
            product_name = row[1]
            input_urls = row[2].split(',')

            input_data.append({
                "serial_number": serial_number,
                "product_name": product_name,
                "input_urls": input_urls
            })

            # Process each image asynchronously
            output_urls = []
            for url in input_urls:
                try:
                    img_resp = requests.get(url)
                    if img_resp.status_code == 200:
                        # Compress the image
                        compressed_image = compress_image(img_resp.content)
                        # Save compressed image to GridFS
                        output_image_id = fs.put(compressed_image)
                        output_urls.append(f"/images/{output_image_id}")
                    else:
                        output_urls.append("Error downloading image")
                except Exception as e:
                    output_urls.append(f"Error: {str(e)}")

            output_data.append({
                "serial_number": serial_number,
                "product_name": product_name,
                "output_urls": output_urls
            })

    # Update MongoDB with the results
    requests_collection.update_one(
        {"request_id": request_id},
        {"$set": {"status": "Completed", "input_data": input_data, "output_data": output_data}}
    )

    # Trigger the webhook if exists
    if request_data.get('webhook_url'):
        requests.post(request_data['webhook_url'], json={"request_id": request_id, "status": "Completed"})

    # Clean up temporary CSV file
    os.remove(file_path)
