import json
import boto3
import logging
import base64
from botocore.exceptions import ClientError
from datetime import datetime
from decimal import Decimal

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table_name = 'bolt_data'
table = dynamodb.Table(table_name)

# Define required columns for validation
TRIP_START_COLUMNS = {
    'trip_id': str,
    'pickup_location_id': int,
    'dropoff_location_id': int,  # Estimated dropoff location id
    'vendor_id': int,
    'pickup_datetime': str,
    'estimated_dropoff_datetime': str,  # Estimated dropoff time
    'estimated_fare_amount': float
}

TRIP_END_COLUMNS = {
    'trip_id': str,
    'dropoff_datetime': str,  # Actual dropoff time
    'rate_code': float,
    'passenger_count': float,
    'trip_distance': float,
    'fare_amount': float,
    'tip_amount': float,
    'payment_type': float,
    'trip_type': float
}


def validate_columns(data, required_schema):
    for col, col_type in required_schema.items():
        if col not in data:
            raise ValueError(f"Missing required column: {col}")
        val = data[col]
        # Type check with some flexibility for numeric types
        if col_type == float:
            try:
                float(val)
            except (ValueError, TypeError):
                raise ValueError(f"Column {col} must be a number, got {type(val).__name__} with value '{val}'")
        elif col_type == int:
            try:
                int(float(val))
            except (ValueError, TypeError):
                raise ValueError(f"Column {col} must be an integer, got {type(val).__name__} with value '{val}'")
        elif col_type == str:
            if not isinstance(val, str):
                raise ValueError(f"Column {col} must be a string, got {type(val).__name__} with value '{val}'")
    return True


def parse_iso8601(dt_str):
    try:
        return datetime.fromisoformat(dt_str)
    except Exception:
        raise ValueError(f"Invalid datetime format: {dt_str}. Expected ISO 8601.")


def extract_trip_date(datetime_str):
    # Extract date portion from ISO8601 datetime string, e.g. "2024-05-25T13:19:00" -> "2024-05-25"
    if not datetime_str or not isinstance(datetime_str, str):
        raise ValueError("Invalid datetime string for trip_date extraction.")
    return datetime_str.split('T')[0]


def process_trip_event(event_data):
    trip_id = event_data.get('trip_id')
    if not trip_id:
        raise ValueError("Missing 'trip_id' in event data.")

    logger.info(f"Processing event for trip_id: {trip_id}")

    try:
        # Determine event type based on keys
        if 'pickup_location_id' in event_data and 'dropoff_datetime' not in event_data:
            logger.info(f"Identified as trip START event for trip_id: {trip_id}")
            validate_columns(event_data, TRIP_START_COLUMNS)

            # Extract trip_date from pickup_datetime
            trip_date = extract_trip_date(event_data['pickup_datetime'])

            last_updated = parse_iso8601(event_data['pickup_datetime']).isoformat()

            update_expression = (
                "SET pickup_location_id = :pickup_location_id, "
                "dropoff_location_id = :dropoff_location_id, "
                "vendor_id = :vendor_id, "
                "pickup_datetime = :pickup_datetime, "
                "estimated_dropoff_datetime = :estimated_dropoff_datetime, "
                "estimated_fare_amount = :estimated_fare_amount, "
                "trip_date = :trip_date, "
                "last_updated = :last_updated"
            )

            expression_values = {
                ':pickup_location_id': int(event_data['pickup_location_id']),
                ':dropoff_location_id': int(event_data['dropoff_location_id']),
                ':vendor_id': int(event_data['vendor_id']),
                ':pickup_datetime': event_data['pickup_datetime'],
                ':estimated_dropoff_datetime': event_data['estimated_dropoff_datetime'],
                ':estimated_fare_amount': Decimal(str(event_data['estimated_fare_amount'])),
                ':trip_date': trip_date,
                ':last_updated': last_updated
            }

        elif 'dropoff_datetime' in event_data:
            logger.info(f"Identified as trip END event for trip_id: {trip_id}")
            validate_columns(event_data, TRIP_END_COLUMNS)

            # Extract trip_date from dropoff_datetime
            trip_date = extract_trip_date(event_data['dropoff_datetime'])

            last_updated = parse_iso8601(event_data['dropoff_datetime']).isoformat()

            update_expression = (
                "SET dropoff_datetime = :dropoff_datetime, "
                "rate_code = :rate_code, "
                "passenger_count = :passenger_count, "
                "trip_distance = :trip_distance, "
                "fare_amount = :fare_amount, "
                "tip_amount = :tip_amount, "
                "payment_type = :payment_type, "
                "trip_type = :trip_type, "
                "trip_date = :trip_date, "
                "last_updated = :last_updated"
            )

            expression_values = {
                ':dropoff_datetime': event_data['dropoff_datetime'],
                ':rate_code': Decimal(str(event_data['rate_code'])),
                ':passenger_count': Decimal(str(event_data['passenger_count'])),
                ':trip_distance': Decimal(str(event_data['trip_distance'])),
                ':fare_amount': Decimal(str(event_data['fare_amount'])),
                ':tip_amount': Decimal(str(event_data['tip_amount'])),
                ':payment_type': Decimal(str(event_data['payment_type'])),
                ':trip_type': Decimal(str(event_data['trip_type'])),
                ':trip_date': trip_date,
                ':last_updated': last_updated
            }

        else:
            raise ValueError(f"Event for trip_id {trip_id} does not match trip start or trip end schema. Keys: {event_data.keys()}")

        logger.info(f"Attempting DynamoDB update for trip_id: {trip_id}. Update expression: {update_expression}")

        response = table.update_item(
            Key={'trip_id': trip_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_values,
            ConditionExpression="attribute_not_exists(last_updated) OR last_updated < :last_updated",
            ReturnValues="ALL_NEW"
        )

        logger.info(f"DynamoDB update successful for trip_id {trip_id}. Updated attributes: {response.get('Attributes', {})}")
        return {'trip_id': trip_id, 'status': 'updated', 'attributes': response.get('Attributes', {})}

    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.warning(f"Stale update ignored for trip_id {trip_id} (ConditionalCheckFailedException).")
            return {'trip_id': trip_id, 'status': 'stale_update_ignored'}
        else:
            logger.error(f"DynamoDB ClientError for trip_id {trip_id}: {e}")
            raise
    except ValueError as ve:
        logger.error(f"Validation or parsing error for trip_id {trip_id}: {ve}")
        raise
    except Exception as ex:
        logger.error(f"Unexpected error for trip_id {trip_id}: {ex}")
        raise


def lambda_handler(event, context):
    logger.info(f"Received Kinesis event with {len(event.get('Records', []))} records.")

    results = []
    errors = []

    for record in event.get('Records', []):
        try:
            payload_base64 = record['kinesis']['data']
            payload = base64.b64decode(payload_base64).decode('utf-8')
            logger.info(f"Decoded Kinesis payload: {payload}")

            data = json.loads(payload)

            result = process_trip_event(data)
            results.append(result)
        except Exception as e:
            error_details = {
                'error': str(e),
                'kinesis_sequence_number': record.get('kinesis', {}).get('sequenceNumber'),
                'kinesis_partition_key': record.get('kinesis', {}).get('partitionKey'),
                'payload_sample': payload[:200] if 'payload' in locals() else 'N/A'
            }
            logger.error(f"Error processing Kinesis record: {json.dumps(error_details)}")
            errors.append(error_details)

    response_body = {
        'processed_records_count': len(results),
        'failed_records_count': len(errors),
        'results': results,
        'errors_summary': errors
    }

    status_code = 200
    if errors:
        status_code = 200  

    logger.info(f"Lambda execution finished. Response: {json.dumps(response_body)}")
    return {
        'statusCode': status_code,
        'body': json.dumps(response_body)
    }
