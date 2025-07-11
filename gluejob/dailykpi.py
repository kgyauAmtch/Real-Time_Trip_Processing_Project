import sys
import boto3
import json
from collections import defaultdict
from decimal import Decimal
from awsglue.utils import getResolvedOptions

# Parse Glue job parameters
args = getResolvedOptions(sys.argv, ['DYNAMODB_TABLE', 'S3_OUTPUT_PATH'])

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(args['DYNAMODB_TABLE'])

s3 = boto3.client('s3')

def scan_all_items(table):
    items = []
    response = table.scan()
    items.extend(response.get('Items', []))
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response.get('Items', []))
    return items

def calculate_kpis(items):
    kpis = defaultdict(lambda: {
        'count_trips': 0,
        'total_fare': Decimal('0'),
        'min_fare': None,
        'max_fare': None
    })

    for item in items:
        if not item.get('trip_completed', False):
            continue
        trip_date = item.get('trip_date')
        fare_amount = item.get('fare_amount')
        if trip_date is None or fare_amount is None:
            continue

        fare = Decimal(str(fare_amount))
        kpi = kpis[trip_date]
        kpi['count_trips'] += 1
        kpi['total_fare'] += fare
        if kpi['min_fare'] is None or fare < kpi['min_fare']:
            kpi['min_fare'] = fare
        if kpi['max_fare'] is None or fare > kpi['max_fare']:
            kpi['max_fare'] = fare

    # Calculate averages
    for date, kpi in kpis.items():
        count = kpi['count_trips']
        kpi['average_fare'] = (kpi['total_fare'] / count).quantize(Decimal('0.01')) if count > 0 else Decimal('0.00')
        kpi['total_fare'] = kpi['total_fare'].quantize(Decimal('0.01'))
        kpi['min_fare'] = kpi['min_fare'].quantize(Decimal('0.01')) if kpi['min_fare'] is not None else None
        kpi['max_fare'] = kpi['max_fare'].quantize(Decimal('0.01')) if kpi['max_fare'] is not None else None

    return kpis

def write_kpi_to_s3(s3_path, trip_date, kpi):
    bucket = s3_path.replace("s3://", "").split('/')[0]
    prefix = '/'.join(s3_path.replace("s3://", "").split('/')[1:]).rstrip('/')
    s3_key = f"{prefix}/date={trip_date}/kpi.json"

    kpi_json = json.dumps({
        'date': trip_date,
        'count_trips': kpi['count_trips'],
        'total_fare': float(kpi['total_fare']),
        'min_fare': float(kpi['min_fare']) if kpi['min_fare'] is not None else None,
        'max_fare': float(kpi['max_fare']) if kpi['max_fare'] is not None else None,
        'average_fare': float(kpi['average_fare'])
    }, indent=2)

    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=kpi_json,
        ContentType='application/json'
    )
    print(f"Wrote KPIs for {trip_date} to s3://{bucket}/{s3_key}")

def main():
    print(f"Starting KPI calculation for DynamoDB table: {args['DYNAMODB_TABLE']}")
    items = scan_all_items(table)
    print(f"Scanned {len(items)} items from DynamoDB")

    kpis = calculate_kpis(items)
    print(f"Calculated KPIs for {len(kpis)} dates")

    for trip_date, kpi in sorted(kpis.items()):
        print(f"Date: {trip_date}")
        print(f"  Count Trips: {kpi['count_trips']}")
        print(f"  Total Fare: {kpi['total_fare']}")
        print(f"  Min Fare: {kpi['min_fare']}")
        print(f"  Max Fare: {kpi['max_fare']}")
        print(f"  Average Fare: {kpi['average_fare']}")
        print()

        write_kpi_to_s3(args['S3_OUTPUT_PATH'], trip_date, kpi)

if __name__ == "__main__":
    main()
