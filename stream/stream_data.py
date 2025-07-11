import boto3
import json
import time
import pandas as pd

# AWS Kinesis config
STREAM_NAME = 'bolt_streams'
REGION = 'eu-north-1'

# CSV file paths
TRIP_START_CSV = '/Users/gyauk/Downloads/Project 7/data/trip_start.csv'
TRIP_END_CSV = '/Users/gyauk/Downloads/Project 7/data/trip_end.csv'

# Initialize Kinesis client
kinesis_client = boto3.client('kinesis', region_name=REGION)

def load_csv(file_path):
    return pd.read_csv(file_path)

def interleave_rows(df1, df2):
    # Interleave rows from two dataframes based on index
    max_len = max(len(df1), len(df2))
    rows = []
    for i in range(max_len):
        if i < len(df1):
            rows.append(('start', df1.iloc[i].to_dict()))
        if i < len(df2):
            rows.append(('end', df2.iloc[i].to_dict()))
    return rows

def send_record_to_kinesis(record_type, record_data):
    # Add a field to distinguish event type
    record_data['event_type'] = record_type

    # Convert to JSON string
    data_str = json.dumps(record_data)

    # Use trip_id as partition key for ordering
    partition_key = record_data.get('trip_id')
    if not partition_key:
        partition_key = 'unknown'

    response = kinesis_client.put_record(
        StreamName=STREAM_NAME,
        Data=data_str.encode('utf-8'),
        PartitionKey=partition_key
    )
    return response

def main():
    # Load CSV files
    trip_start_df = load_csv(TRIP_START_CSV)
    trip_end_df = load_csv(TRIP_END_CSV)

    # Interleave rows
    interleaved = interleave_rows(trip_start_df, trip_end_df)

    print(f"Streaming {len(interleaved)} records to Kinesis stream '{STREAM_NAME}'...")

    # Stream records with a small delay to simulate real-time
    for event_type, record in interleaved:
        try:
            resp = send_record_to_kinesis(event_type, record)
            print(f"Sent {event_type} event for trip_id={record.get('trip_id')} to shard {resp['ShardId']}")
        except Exception as e:
            print(f"Error sending record: {e}")
        time.sleep(0.05)  # Adjust delay as needed

    print("Streaming complete.")

if __name__ == "__main__":
    main()
