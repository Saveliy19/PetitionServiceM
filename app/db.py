from pymongo import MongoClient

from app.config import settings

client = MongoClient(settings.DB_HOST)
db = client['Petitions']