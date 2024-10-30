from collections import Counter
from datetime import datetime, timedelta
import csv

# List of time ranges
# time_ranges = {
#     "ARW4BT":[("Jun 10 2024 13:00", "Jun 13 2024 12:00"),
#                 ("Jun 14 2024 13:00", "Jun 26 2024 13:00"),
#                 ("Jun 10 2024 11:00", "Jun 11 2024 10:00"),
#                 ("Jun 12 2024 11:00", "Jun 15 2024 11:00"),
#                 ("Jun 10 2024 9:00", "Jun 20 2024 10:00"),
#                 ("Jun 21 2024 9:00", "Jun 30 2024 9:00"),
#                 ("Jun 10 2024 19:00", "Jun 20 2024 18:00"),
#                 ("Jun 21 2024 19:00", "Jul 1 2024 19:00"),
#                 ("Jun 13 2024 17:00", "Jun 16 2024 16:00"),
#                 ("Jun 16 2024 23:00", "Jun 25 2024 23:00"),
#                 ("Jun 10 2024 14:00", "Jun 16 2024 13:00"),
#                 ("Jun 17 2024 14:00", "Jun 22 2024 14:00")
#     ], 
#     "ARW3BT": [("Jun 13 2024 13:00", "Jun 14 2024 12:00"), 
#                ("Jun 11 2024 11:00", "Jun 12 2024 10:00"), 
#                ("Jun 20 2024 11:00", "Jun 21 2024 8:00"), 
#                ("Jun 20 2024 19:00", "Jun 21 2024 18:00"),
#                ("Jun 16 2024 17:00", "Jun 16 2024 22:00"),
#                ("Jun 16 2024 14:00", "Jun 17 2024 13:00")
#     ],
#     "ARW3S" :[("Jun 10 2024 17:00", "Jun 13 2024 16:00")
#     ]
#     }

time_ranges = {
    "ED": [("Jun 01 2024 13:00", "Jun 02 2024 22:00"),
           ("Jun 01 2024 22:00","Jun 03 2024 14:00"),
           ("Jun 01 2024 23:00", "Jun 05 2024 8:00"),
           ("May 29 2024 14:00", "Jun 02 2024 21:00")],
    "PAT": [("Jun 02 2024 23:00", "Jun 04 2024 8:00"),
            ("Jun 3 2024 15:00", "Jun 04 2024 15:00"),
            ("Jun 06 2024 9:00","Jun 08 2024 16:00"),
            ("Jun 05 2024 9:00", "Jun 06 2024 10:00"),
            ("Jun 02 2024 22:00", "Jun 06 2024 11:00"),
            ("Jun 06 2024 10:00", "Jun 07 2024 8:00")],
    "MEDTELE": [("Jun 05 2024 19:00", "Jun 15 2024 10:00"),
                ("Jun 05 2024 1:00", "Jun 1 2024 13:00"),
                ("Jun 03 2024 18:00", "Jun 06 2024 8:00"),
                ("Jun 8 2024 17:00", "Jun 02 2024 17:00"),
                ("Jun 7 2024 3:00", "Jun 01 2024 10:00" ),
                ("Jun 07 2024 11:00", "Jun 20 2024 15:00")],
    "MED": [("Jun 04 2024 23:00", "Jun 20 2024 15:00"), 
            ("Jun 04 2024 19:00", "Jun 18 2024 15:00"),
            ("Jun 06 2024 23:00", "Jun 15 2024 20:00"),
            ("Jun 08 2024 18:00", "Jun 20 2024 14:00")],
    "ICU": [("Jun 03 2024 1:00", "Jun 04 2024 9:00"),
            ("Jun 03 2024 9:00", "Jun 04 2024 9:00"),
            ("Jun 03 2024 19:00", "Jun 06 2024 4:00"),
            ("Jun 03 2024 9:00", "Jun 06 2024 9:00")],
    "OR":[("Jun 04 2024 9:00", "Jun 05 2024 18:00"), 
          ("Jun 04 2024 10:00", "Jun 04 2024 22:00"),
          ("Jun 04 2024 10:00","Jun 04 2024 18:00"),
          ("Jun 04 2024 16:00", "Jun 05 2024 00:00"), 
          ("Jun 06 2024 5:00", "Jun 06 2024 22:00"),
          ("Jun 06 2024 11:00", "Jun 07 2024 2:00"),
          ("Jun 06 2024 12:00", "Jun 07 2024 10:00"),
          ("Jun 07 2024 9:00", "Jun 08 2024 17:00")
          ]
}
# Convert string to datetime object
def convert_to_datetime(date_str):
    return datetime.strptime(date_str, '%b %d %Y %H:%M')

# Function to generate hourly timestamps for a given range
def generate_hourly_timestamps(start, end):
    start_dt = convert_to_datetime(start)
    end_dt = convert_to_datetime(end)
    timestamps = []
    current_dt = start_dt.replace(minute= 0,second=0, microsecond=0 )
    while current_dt < end_dt:
        timestamps.append(current_dt)
        current_dt += timedelta(hours=1)
    return timestamps

key_timestamp_counts = {}

# Generate hourly timestamps for each range in each set
for key, ranges in time_ranges.items():
    all_timestamps = []
    for start, end in ranges:
        all_timestamps.extend(generate_hourly_timestamps(start, end))
    
    # Count the occurrences of each timestamp for the current key
    key_timestamp_counts[key] = Counter(all_timestamps)
print(key_timestamp_counts)

with open('filtered_timestamp_counts.csv', 'w', newline='') as csvfile:
    fieldnames = ['POC', 'Timestamp', 'Count']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for key, counts in key_timestamp_counts.items():
        for timestamp, count in counts.items():
            formatted_timestamp = timestamp.strftime('%m/%d/%Y  %I:%M:%S %p')
            writer.writerow({'POC': key, 'Timestamp': formatted_timestamp, 'Count': count})

print("CSV file 'filtered_timestamp_counts_3.csv' has been created.")


