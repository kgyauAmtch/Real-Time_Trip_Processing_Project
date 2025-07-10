import json
import boto3
import logging
from decimal import Decimal

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
kpi_table = dynamodb.Table('TripDailyKPIs')  # Table for daily KPI aggregates

def lambda_handler(event, context):
    logger.info(f"Received {len(event.get('Records', []))} DynamoDB stream records")

    for record in event['Records']:
        event_name = record.get('eventName')
        logger.info(f"Processing record with eventName: {event_name}")

        # Process only INSERT or MODIFY events
        if event_name not in ['INSERT', 'MODIFY']:
            logger.info("Skipping record as eventName is not INSERT or MODIFY")
            continue

        new_image = record['dynamodb'].get('NewImage', {})
        old_image = record['dynamodb'].get('OldImage', {})

        has_new_dropoff = 'dropoff_datetime' in new_image
        had_old_dropoff = 'dropoff_datetime' in old_image

        logger.info(f"New image has dropoff_datetime: {has_new_dropoff}, Old image had dropoff_datetime: {had_old_dropoff}")

        # Only process if dropoff_datetime just appeared (trip completion)
        if not has_new_dropoff or had_old_dropoff:
            logger.info("Skipping record as it does not represent a new trip completion")
            continue

        # Extract trip_date from the new image
        trip_date = new_image.get('trip_date', {}).get('S')
        if not trip_date:
            logger.warning("trip_date attribute missing in new image; skipping record")
            continue

        # Extract fare_amount safely
        fare_amount_str = new_image.get('fare_amount', {}).get('N', '0')
        try:
            fare_amount = float(fare_amount_str)
        except ValueError:
            logger.error(f"Invalid fare_amount value: {fare_amount_str}; skipping record")
            continue

        logger.info(f"Updating KPIs for trip_date: {trip_date} with fare_amount: {fare_amount}")

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

    logger.info("Incremental KPI update complete")
    return {
        'statusCode': 200,
        'body': json.dumps('Incremental KPI update complete')
    }
