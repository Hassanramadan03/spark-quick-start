from pymongo import MongoClient

# MongoDB URI (replace with your actual connection string)
mongo_uri = "mongodb+srv://eslam:sb9hufbS1ds8z52v@cluster0.0zicw.mongodb.net/analytics_db.detections?retryWrites=true&w=majority&appName=Cluster0"

try:
    # Create MongoDB client
    client = MongoClient(mongo_uri)

    # Access the database and collection
    db = client['analytics_db']        # Replace with your database name
    collection = db['detections']      # Replace with your collection name

    # Test insert a sample document
    test_document = {"test_key": "test_value"}
    result = collection.insert_one(test_document)
    print("Document inserted with ID:", result.inserted_id)

    # Retrieve the inserted document to confirm
    retrieved_document = collection.find_one({"_id": result.inserted_id})
    print("Retrieved document:", retrieved_document)

    print("MongoDB connection successful!")
except Exception as e:
    print("Failed to connect to MongoDB:", e)
finally:
    # Close the MongoDB connection
    client.close()
