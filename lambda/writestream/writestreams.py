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
    'trip_id': str,
    'dropoff_datetime': str,
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
        if col_type == float:
            try:
                float(val)
            except (ValueError, TypeError):
                raise ValueError(f"Column {col} must be a number, got {type(val).__name__}")
        elif col_type == int:
            try:
                int(float(val))
            except (ValueError, TypeError):
                raise ValueError(f"Column {col} must be an integer, got {type(val).__name__}")
        elif col_type == str:
            if not isinstance(val, str):
                raise ValueError(f"Column {col} must be a string, got {type(val).__name__}")
    return True


def parse_iso8601(dt_str):
    return datetime.fromisoformat(dt_str)


def extract_trip_date(datetime_str):
    if not datetime_str or not isinstance(datetime_str, str):
        raise ValueError("Invalid datetime string for trip_date extraction.")
    return datetime_str.split('T')[0]


def process_trip_event(event_data):
    trip_id = event_data.get('trip_id')
    if not trip_id:
        raise ValueError("Missing 'trip_id' in event data.")

    logger.info(f"Processing event for trip_id: {trip_id}")
    try:
        expression_values = {}
        is_start_event = 'pickup_location_id' in event_data and 'dropoff_datetime' not in event_data
        is_end_event = 'dropoff_datetime' in event_data

        update_expression_parts = []
        update_expression = "SET "
        last_updated = None

        if is_start_event:
            logger.info(f"Identified as trip START event for trip_id: {trip_id}")
            validate_columns(event_data, TRIP_START_COLUMNS)

            last_updated = parse_iso8601(event_data['pickup_datetime']).isoformat()
            trip_date_start = extract_trip_date(event_data['pickup_datetime'])

            expression_values.update({
                ':pickup_location_id': int(event_data['pickup_location_id']),
                ':dropoff_location_id': int(event_data['dropoff_location_id']),
                ':vendor_id': int(event_data['vendor_id']),
                ':pickup_datetime': event_data['pickup_datetime'],
                ':estimated_dropoff_datetime': event_data['estimated_dropoff_datetime'],
                ':estimated_fare_amount': Decimal(str(event_data['estimated_fare_amount'])),
                ':trip_date_start': trip_date_start,
                ':last_updated': last_updated
            })

            update_expression_parts.extend([
                "pickup_location_id = :pickup_location_id",
                "dropoff_location_id = :dropoff_location_id",
                "vendor_id = :vendor_id",
                "pickup_datetime = :pickup_datetime",
                "estimated_dropoff_datetime = :estimated_dropoff_datetime",
                "estimated_fare_amount = :estimated_fare_amount",
                "trip_date_start = :trip_date_start",
                "last_updated = :last_updated"
            ])

        elif is_end_event:
            logger.info(f"Identified as trip END event for trip_id: {trip_id}")
            validate_columns(event_data, TRIP_END_COLUMNS)

            last_updated = parse_iso8601(event_data['dropoff_datetime']).isoformat()
            trip_date_end = extract_trip_date(event_data['dropoff_datetime'])

            expression_values.update({
                ':dropoff_datetime': event_data['dropoff_datetime'],
                ':rate_code': Decimal(str(event_data['rate_code'])),
                ':passenger_count': Decimal(str(event_data['passenger_count'])),
                ':trip_distance': Decimal(str(event_data['trip_distance'])),
                ':fare_amount': Decimal(str(event_data['fare_amount'])),
                ':tip_amount': Decimal(str(event_data['tip_amount'])),
                ':payment_type': Decimal(str(event_data['payment_type'])),
                ':trip_type': Decimal(str(event_data['trip_type'])),
                ':trip_date_end': trip_date_end,
                ':last_updated': last_updated
            })

            update_expression_parts.extend([
                "dropoff_datetime = :dropoff_datetime",
                "rate_code = :rate_code",
                "passenger_count = :passenger_count",
                "trip_distance = :trip_distance",
                "fare_amount = :fare_amount",
                "tip_amount = :tip_amount",
                "payment_type = :payment_type",
                "trip_type = :trip_type",
                "trip_date_end = :trip_date_end",
                "last_updated = :last_updated"
            ])

        else:
            raise ValueError(f"Event for trip_id {trip_id} does not match expected schemas.")

        # Finalize update expression
        update_expression += ", ".join(update_expression_parts)

        # Perform the update
        response = table.update_item(
            Key={'trip_id': trip_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_values,
            ConditionExpression="attribute_not_exists(last_updated) OR last_updated < :last_updated",
            ReturnValues="ALL_NEW"
        )

        updated_attributes = response.get('Attributes', {})
        logger.info(f"Update succeeded. Checking for trip completion for trip_id: {trip_id}")

        # Determine if trip is now complete
        if updated_attributes.get('pickup_datetime') and updated_attributes.get('dropoff_datetime'):
            table.update_item(
                Key={'trip_id': trip_id},
                UpdateExpression="SET trip_completed = :true_val",
                ExpressionAttributeValues={':true_val': True}
            )
            updated_attributes['trip_completed'] = True
            logger.info(f"Marked trip_id {trip_id} as completed.")

        return {'trip_id': trip_id, 'status': 'updated', 'attributes': updated_attributes}

    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.warning(f"Stale update ignored for trip_id {trip_id}.")
            return {'trip_id': trip_id, 'status': 'stale_update_ignored'}
        else:
            logger.error(f"DynamoDB error for trip_id {trip_id}: {e}")
            raise
    except Exception as ex:
        logger.error(f"Error processing trip_id {trip_id}: {ex}")
        raise


def lambda_handler(event, context):
    logger.info(f"Received Kinesis event with {len(event.get('Records', []))} records.")

    results = []
    errors = []

    for record in event.get('Records', []):
        try:
            payload_base64 = record['kinesis']['data']
            payload = base64.b64decode(payload_base64).decode('utf-8')
            logger.info(f"Decoded payload: {payload}")

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
            logger.error(f"Error processing record: {json.dumps(error_details)}")
            errors.append(error_details)

    return {
        'statusCode': 200,
        'body': json.dumps({
            'processed_records_count': len(results),
            'failed_records_count': len(errors),
            'results': results,
            'errors_summary': errors
        })
    }