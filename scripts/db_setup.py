from pymongo import MongoClient


client = MongoClient()
db = client['Petitions']

db.petitions.drop()
db.comments.drop()
db.likes.drop()
db.photo_folders.drop()

db.create_collection("petitions")
db.create_collection("comments")
db.create_collection("likes")
db.create_collection("photo_folders")
