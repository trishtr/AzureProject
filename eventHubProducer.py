import csv
import pandas as pd
from azure.eventhub import EventHubProducerClient, EventData, EventHubConsumerClient

connection_str = ""

consumer_group = '$Default'
eventhub_name = 'test-adf-1'


# with capture feature on to automatically write messages to blob
def read_messages_from_csv(file_path, column_name):
    df = pd.read_csv(file_path, delimiter=',', error_bad_lines=False)
    messages = df[column_name]
    return messages

def encode_decode_message(message):
    # Replace `\r` with space to format as required
    # formatted_message = message.replace(r'\r', '\n')
    # Replace escaped characters like `\\` with their single counterparts
    # formatted_message = formatted_message.replace(r'\\', '\\')
    formatted_message = message.encode('utf-8').decode('unicode_escape')

    return formatted_message


def send_messages_from_csv(file_path):
    # Create a producer client to send messages to the Event Hub
    producer = EventHubProducerClient.from_connection_string(
        connection_str,
        eventhub_name=eventhub_name
    )

    # Read messages from the CSV file
    messages = read_messages_from_csv(file_path,column_name)

    # Create a batch to hold messages
    event_data_batch = producer.create_batch()

    try:
        # Add messages to the batch
        for message in messages:
            formatted_message = encode_decode_message(message)
            print(formatted_message)

            # If adding the message exceeds the batch size, send the batch and start a new one
            try:
                event_data_batch.add(EventData(formatted_message))
                event_data_batch.properties = {'Content-Type': 'text/plain'}

            except ValueError:
                producer.send_batch(event_data_batch)
                event_data_batch = producer.create_batch()
                event_data_batch.add(EventData(formatted_message))
                
        
        # Send the last batch if it has any messages
        if len(event_data_batch) > 0:
            producer.send_batch(event_data_batch)
        
        print("Messages sent successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the producer
        producer.close()





if __name__ == "__main__":
    column_name = 'dataset'
    file_path = './test-adf-1.csv'  # Replace with the path to CSV file
    read_messages_from_csv(file_path,column_name)
    send_messages_from_csv(file_path)
 
