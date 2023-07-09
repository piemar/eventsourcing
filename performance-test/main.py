import random
import uuid
from locust import HttpUser, task, between
from pymongo import MongoClient
from datetime import datetime

class MongoUser(HttpUser):
    wait_time = between(0.3, 0.8)  # Adjust the wait time according to your needs
    user_count = 0
    version_id = str(uuid.uuid4())
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = MongoClient('mongodb+srv://<user>:<password>@demo-cluster.tcrpd.mongodb.net/')  # Replace with your MongoDB Atlas connection URI

    def on_start(self):
        # Initialize any required setup or authentication for your MongoDB Atlas cluster
        pass

    @task
    def insert_document(self):
        if self.user_count % 100 == 0:
            self.version_id = str(uuid.uuid4())  # Generate a new GUID (UUID) for VersionId

        event_id = generate_event_id()  # Function to generate a new EventId
        timestamp = datetime.now() # Current timestamp in ISO format

        document = {
            'Event': 'VersionUpdated',
            'AggregateId': self.version_id,
            'Timestamp': timestamp,
            'EventId': event_id,
            'Payload': {
                'RState': 'R1A',
                'Description': 'Updated Test Description Again'
            }
        }

        # Insert the document into your MongoDB Atlas cluster
        db = self.client['aurora']  # Replace with your database name
        collection = db['events']  # Replace with your collection name
        collection.insert_one(document)
        self.user_count = self.user_count+1

def generate_event_id():
    # Implement your EventId generation logic here
    # This is just a simple example using random numbers
    return random.randint(1, 1000)
