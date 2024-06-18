import requests
import sqlite3
from datetime import datetime, timedelta
import json
import os

# Constants
BART_API_URL = 'http://api.bart.gov/api/sched.aspx'
BART_API_KEY = os.getenv('BART_API_KEY')
DB_PATH = 'bart_schedule.db'

def create_db():
    """Create the database and table if it doesn't exist."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS schedules (
            train_id TEXT PRIMARY KEY,
            origin TEXT,
            destination TEXT,
            departure_time TEXT,
            arrival_time TEXT,
            UNIQUE(train_id)
        )
    ''')
    conn.commit()
    conn.close()

def get_existing_schedules():
    """Get existing schedules from the database for the next 24 hours."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    next_24_hours = datetime.now() + timedelta(hours=24)
    cursor.execute('''
        SELECT * FROM schedules WHERE datetime(departure_time) < ?
    ''', (next_24_hours,))
    rows = cursor.fetchall()
    conn.close()
    return rows

def fetch_bart_schedule():
    """Fetch the BART schedule from the API."""
    params = {
        'cmd': 'depart',
        'orig': 'ALL',  # You may need to specify a particular origin
        'key': BART_API_KEY,
        'b': 0,
        'a': 24,  # Next 24 hours
        'json': 'y'
    }
    response = requests.get(BART_API_URL, params=params)
    data = response.json()
    return data

def parse_schedule_data(data):
    """Parse the schedule data from the API response."""
    schedules = []
    for schedule in data['root']['station']:
        for item in schedule['etd']:
            for estimate in item['estimate']:
                schedules.append({
                    'train_id': estimate['trainHeadStation'],
                    'origin': schedule['name'],
                    'destination': item['abbreviation'],
                    'departure_time': estimate['minutes'],
                    'arrival_time': estimate['length'],
                })
    return schedules

def upsert_schedules(new_schedules):
    """Upsert the new schedules into the database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    for schedule in new_schedules:
        cursor.execute('''
            INSERT OR REPLACE INTO schedules (train_id, origin, destination, departure_time, arrival_time)
            VALUES (?, ?, ?, ?, ?)
        ''', (schedule['train_id'], schedule['origin'], schedule['destination'],
              schedule['departure_time'], schedule['arrival_time']))
    conn.commit()
    conn.close()

def calculate_difference(existing, new):
    """Calculate the difference between existing and new schedules."""
    existing_set = set((row[0], row[1], row[2], row[3], row[4]) for row in existing)
    new_set = set((item['train_id'], item['origin'], item['destination'], item['departure_time'], item['arrival_time']) for item in new)
    difference = new_set - existing_set
    return [dict(zip(['train_id', 'origin', 'destination', 'departure_time', 'arrival_time'], item)) for item in difference]

def main():
    create_db()
    existing_schedules = get_existing_schedules()
    api_response = fetch_bart_schedule()
    new_schedules = parse_schedule_data(api_response)
    difference = calculate_difference(existing_schedules, new_schedules)
    if difference:
        upsert_schedules(difference)
        print(f"Upserted {len(difference)} new/updated schedules.")
    else:
        print("No new schedules to upsert.")

if __name__ == "__main__":
    main()
