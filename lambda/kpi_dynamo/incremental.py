import json
import boto3
import logging
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
kpi_table = dynamodb.Table('TripDailyKPIs')

def lambda_handler(event, context):
    logger.info(f"Received {len(event.get('Records', []))} DynamoDB stream records")

    for record in event['Records']:
        event_name = record.get('eventName')
        logger.info(f"Processing record with eventName: {event_name}")

        if event_name not in ['INSERT', 'MODIFY']:
            continue

        new_image = record['dynamodb'].get('NewImage', {})
        if not new_image:
            continue

        # Only process records where trip_completed = true
        trip_completed = new_image.get('trip_completed', {}).get('BOOL', False)
        if not trip_completed:
            logger.info("Skipping record because trip is not marked as completed")
            continue

        # Use trip_date_end for completed trips
        trip_date = new_image.get('trip_date_end', {}).get('S')
        if not trip_date:
            logger.warning("trip_date_end missing; skipping record")
            continue

        fare_amount_str = new_image.get('fare_amount', {}).get('N', '0')
        try:
            fare_amount = float(fare_amount_str)
        except ValueError:
            logger.error(f"Invalid fare_amount value: {fare_amount_str}; skipping")
            continue

        logger.info(f"Updating KPIs for completed trip on {trip_date} with fare_amount: {fare_amount}")

        try:
            response = kpi_table.update_item(
                Key={'trip_date': trip_date},
                UpdateExpression="""
                    SET total_fare = if_not_exists(total_fare, :zero) + :fare,
                        count_trips = if_not_exists(count_trips, :zero) + :one,
                        max_fare = if_not_exists(max_fare, :min_val),
                        min_fare = if_not_exists(min_fare, :max_val)
                """,
                ExpressionAttributeValues={
                    ':fare': Decimal(str(fare_amount)),
                    ':one': Decimal(1),
                    ':zero': Decimal(0),
                    ':min_val': Decimal(str(fare_amount)),
                    ':max_val': Decimal(str(fare_amount))
                },
                ConditionExpression="attribute_not_exists(count_trips) OR :fare > max_fare OR :fare < min_fare OR attribute_exists(count_trips)",
                ReturnValues="UPDATED_NEW"
            )
            logger.info(f"KPI update successful for {trip_date}: {response.get('Attributes')}")
        except Exception as e:
            logger.error(f"Error updating KPI aggregates for {trip_date}: {e}")

    logger.info("KPI aggregation complete.")
    return {
        'statusCode': 200,
        'body': json.dumps('KPI aggregation complete.')
    }