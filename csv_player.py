import csv
import json
import time

from google.cloud import pubsub_v1

# --- IMPORTANT: Make sure your details are correct ---
project_id = "click-stream-463520"
topic_id = "clicks"
# The name of your CSV file - please double-check it!
# I am guessing the file is named '2008.csv' based on the data.
# If it has a different name, change it here.
csv_file_name = "clothing.csv" 
# --- Do not change anything below this line ---

# Create a publisher client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

print(f"Starting CSV Player. Reading from '{csv_file_name}'.")
print(f"Publishing to topic: {topic_path}")
print("Press Ctrl+C to stop.")

try:
    # Open the CSV file to read from it
    with open(csv_file_name, 'r') as file:
        # Use DictReader and tell it the delimiter is a semicolon
        reader = csv.DictReader(file, delimiter=';')
        
        # Loop through each row in the CSV file
         # Loop through each row in the CSV file
        for row in reader:
            # Convert the row (which is a dictionary) into a JSON string
            data_to_send = json.dumps(row).encode("utf-8")
            
            # --- THIS IS THE NEW, UPGRADED PART ---
            print("Attempting to publish...")
            future = publisher.publish(topic_path, data_to_send)
            
            # This line will now WAIT for a response from Google.
            # It will either work, or it will give us a specific error.
            message_id = future.result() 
            
            print(f"SUCCESS! Message published with ID: {message_id}")
            # --- END OF THE NEW PART ---

            # Slowing it down to make the output easier to read
            time.sleep(1) 

except FileNotFoundError:
    print(f"ERROR: The file '{csv_file_name}' was not found.")
    print("Please make sure it's in the same folder as the Python script and the name is correct.")
except KeyboardInterrupt:
    print("\nStopping CSV Player.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
    