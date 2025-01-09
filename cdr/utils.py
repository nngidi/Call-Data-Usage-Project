import json
from datetime import datetime

def process_cdr_data(raw_data):
    # Parse JSON data
    record = json.loads(raw_data)
    print(f"Processing record: {record}")

    # Initialize fields
    msisdn = record.get('msisdn')
    tower_id = record.get('tower_id')
    # Extract either 'event_datetime' or 'start_time', whichever exists
    timestamp = record.get('event_datetime') or record.get('start_time')

    # Parse the timestamp and extract the date
    if timestamp:
        usage_date = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f').date()
    else:
        raise ValueError("No valid timestamp found in the record")

    # Differentiate between data and call records
    if 'data_type' in record:
        usage_type = record.get('data_type')
        up_bytes = int(record.get('up_bytes', 0))
        down_bytes = int(record.get('down_bytes', 0))
        call_cost = 0.0
        data_cost = calculate_data_cost(up_bytes + down_bytes)
    elif 'call_type' in record:
        usage_type = record.get('call_type')
        up_bytes = 0
        down_bytes = 0
        call_duration_sec = int(record.get('call_duration_sec', 0))
        call_cost = calculate_call_cost(call_duration_sec)
        data_cost = 0.0
    else:
        raise KeyError('usage_type')

    # Summarize the record
    summarized_record = {
        'msisdn': msisdn,
        'usage_date': usage_date,
        'usage_type': usage_type,
        'up_bytes': up_bytes,
        'down_bytes': down_bytes,
        'call_cost': call_cost,
        'data_cost': data_cost,
    }
    return [summarized_record]

def calculate_data_cost(total_bytes):
    # Example data cost calculation
    rate_per_byte = 49 / 10**9
    return total_bytes * rate_per_byte

def calculate_call_cost(duration_sec):
    # Example call cost calculation
    rate_per_minute = 1 / 60
    return duration_sec * rate_per_minute
