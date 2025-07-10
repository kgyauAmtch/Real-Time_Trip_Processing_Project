import json
import boto3
import logging
import base64
from botocore.exceptions import ClientError
from datetime import datetime

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
    'dropoff_location_id': int,
    'vendor_id': int,
    'pickup_datetime': str,
    'estimated_dropoff_datetime': str,
    'estimated_fare_amount': float
}

TRIP_END_COLUMNS = {
    'dropoff_datetime': str,
    'rate_code': float,
    'passenger_count': float,
    'trip_distance': float,
    'fare_amount': float,
    'tip_amount': float,
    'payment_type': float,
    'trip_type': float,
    'trip_id': str
}

def validate_columns(data, required_schema):
    for col, col_type in required_schema.items():
        if col not in data:
            raise ValueError(f"Missing required column: {col}")
        val = data[col]
        if col_type == float:
            try:
                float(val)
            except:
                raise ValueError(f"Column {col} must be a number")
        elif col_type == int:
            try:
                int(val)
            except:
                raise ValueError(f"Column {col} must be an integer")
        elif col_type == str:
            if not isinstance(val, str):
                raise ValueError(f"Column {col} must be a string")
    return True

def parse_iso8601(dt_str):
    try:
        return datetime.fromisoformat(dt_str)
    except Exception:
        raise ValueError(f"Invalid datetime format: {dt_str}")

def process_trip_event(event):
    """
    Process a single trip event (dict), validate and update DynamoDB.
    Returns a dict with status and message.
    """
    try:
        if 'dropoff_datetime' in event:
            logger.info("Processing trip start event")
            validate_columns(event, TRIP_START_COLUMNS)
            trip_id = event['trip_id']
            last_updated = parse_iso8601(event['dropoff_datetime']).isoformat()
            update_expression = (
                "SET dropoff_datetime = :dropoff_datetime, "
                "rate_code = :rate_code, passenger_count = :passenger_count, "
                "trip_distance = :trip_distance, fare_amount = :fare_amount, "
                "tip_amount = :tip_amount, payment_type = :payment_type, "
                "trip_type = :trip_type, last_updated = :last_updated"
            )
            expression_values = {
                ':dropoff_datetime': event['dropoff_datetime'],
                ':rate_code': float(event['rate_code']),
                ':passenger_count': float(event['passenger_count']),
                ':trip_distance': float(event['trip_distance']),
                ':fare_amount': float(event['fare_amount']),
                ':tip_amount': float(event['tip_amount']),
                ':payment_type': float(event['payment_type']),
                ':trip_type': float(event['trip_type']),
                ':last_updated': last_updated
            }
        elif 'pickup_location_id' in event:
            logger.info("Processing trip end event")
            validate_columns(event, TRIP_END_COLUMNS)
            trip_id = event['trip_id']
            last_updated = parse_iso8601(event['estimated_dropoff_datetime']).isoformat()
            update_expression = (
                "SET pickup_location_id = :pickup_location_id, "
                "dropoff_location_id = :dropoff_location_id, vendor_id = :vendor_id, "
                "pickup_datetime = :pickup_datetime, "
                "estimated_dropoff_datetime = :estimated_dropoff_datetime, "
                "estimated_fare_amount = :estimated_fare_amount, last_updated = :last_updated"
            )
            expression_values = {
                ':pickup_location_id': int(event['pickup_location_id']),
                ':dropoff_location_id': int(event['dropoff_location_id']),
                ':vendor_id': int(event['vendor_id']),
                ':pickup_datetime': event['pickup_datetime'],
                ':estimated_dropoff_datetime': event['estimated_dropoff_datetime'],
                ':estimated_fare_amount': float(event['estimated_fare_amount']),
                ':last_updated': last_updated
            }
        else:
            raise ValueError("Event does not match trip start or trip end schema")

        logger.info(f"Updating DynamoDB for trip_id: {trip_id} with values: {expression_values}")

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
            logger.warning(f"Stale update ignored for trip_id {event.get('trip_id')}")
            return {'trip_id': event.get('trip_id'), 'status': 'stale_update_ignored'}
        else:
            logger.error(f"DynamoDB ClientError for trip_id {event.get('trip_id')}: {str(e)}")
            raise
    except ValueError as ve:
        logger.error(f"Validation error for trip_id {event.get('trip_id')}: {str(ve)}")
        raise
    except Exception as ex:
        logger.error(f"Unexpected error for trip_id {event.get('trip_id')}: {str(ex)}")
        raise

def lambda_handler(event, context):
    logger.info(f"Received Kinesis event with {len(event.get('Records', []))} records")

    results = []
    errors = []

    for record in event.get('Records', []):
        try:
            # Decode base64 data from Kinesis
            payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            logger.info(f"Decoded payload: {payload}")

            # Parse JSON payload
            data = json.loads(payload)

            # Process each trip event
            result = process_trip_event(data)
            results.append(result)
        except Exception as e:
            logger.error(f"Error processing record: {str(e)}")
            errors.append({'error': str(e), 'record': record})

    response_body = {
        'processed_records': len(results),
        'errors': errors,
        'results': results
    }

    status_code = 200 if not errors else 500

    return {
        'statusCode': status_code,
        'body': json.dumps(response_body)
    }
