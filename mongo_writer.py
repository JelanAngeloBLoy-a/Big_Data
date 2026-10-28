
import json

from pymongo import MongoClient, UpdateOne


def main():
    with open("/app/processed_records.json", "r", encoding="utf-8") as file:
        records = json.load(file)

    if not records:
        print("No records to save.")
        return

    client = MongoClient(
        "mongodb://mongodb:27017",
        serverSelectionTimeoutMS=10000
    )

    try:
        client.admin.command("ping")

        collection = client["bigdata"]["weather_history"]
        operations = []

        for record in records:
            key = {
                "city": record.get("city"),
                "timestamp": record.get("timestamp"),
                "source": record.get("source"),
            }

            operations.append(
                UpdateOne(key, {"$set": record}, upsert=True)
            )

        result = collection.bulk_write(operations, ordered=False)

        print("MongoDB save successful!")
        print("New records:", result.upserted_count)
        print("Updated records:", result.modified_count)
        print("Total stored records:", collection.count_documents({}))

    finally:
        client.close()


if __name__ == "__main__":
    main()
