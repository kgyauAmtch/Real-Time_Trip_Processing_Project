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

def send_batch_to_kinesis(records):
    # records: list of dicts with keys: Data (bytes), PartitionKey (str)
    response = kinesis_client.put_records(
        Records=records,
        StreamName=STREAM_NAME
    )
    failed_count = response['FailedRecordCount']
    if failed_count > 0:
        print(f"Warning: {failed_count} records failed to put in this batch.")
    return response

def stream_records_in_batches(df, record_type, batch_size=200, limit=None, delay=0.1):
    n = min(limit, len(df)) if limit else len(df)
    sampled_df = df.sample(n=n, random_state=42).reset_index(drop=True)

    batch = []
    for i, record in sampled_df.iterrows():
        data = record.to_dict()
        data['event_type'] = record_type
        data_str = json.dumps(data)
        partition_key = data.get('trip_id') or 'unknown'

        batch.append({
            'Data': data_str.encode('utf-8'),
            'PartitionKey': partition_key
        })

        # When batch is full or last record, send batch
        if len(batch) == batch_size or i == n - 1:
            try:
                resp = send_batch_to_kinesis(batch)
                print(f"✓ Sent batch of {len(batch)} {record_type} records.")
            except Exception as e:
                print(f"Error sending batch of {record_type} records: {e}")
            batch = []
            time.sleep(delay)

def main():
    trip_start_df = load_csv(TRIP_START_CSV)
    trip_end_df = load_csv(TRIP_END_CSV)

    print(f"Streaming {len(trip_start_df)} trip_start events in batches...")
    stream_records_in_batches(trip_start_df, 'start', batch_size=100, delay=0.05)

    print("Waiting before sending trip_end events...")
    time.sleep(2)

    print(f"Streaming {len(trip_end_df)} trip_end events in batches...")
    stream_records_in_batches(trip_end_df, 'end', batch_size=100, delay=0.05)

    print("Streaming complete.")

if __name__ == "__main__":
    main()
