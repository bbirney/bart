import requests
import json
import sqlite3
from datetime import datetime
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_bart_data():
    url = "http://api.bart.gov/api/sched.aspx?cmd=depart&json=y&orig=dbrk&dest=embr&b=0&a=24&key=" + os.getenv("BART_API_KEY")
    try:
        response = requests.get(url)
        response.raise_for_status()
        logging.info("Data fetched successfully from BART API.")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data: {e}")
        return None

def create_database():
    try:
        conn = sqlite3.connect('bart_schedule.db')
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS schedule
                          (origin TEXT, destination TEXT, fare REAL, orig_time TEXT, dest_time TEXT, trip_time INTEGER, date TEXT)''')
        conn.commit()
        logging.info("Database created and checked successfully.")
    except sqlite3.Error as e:
        logging.error(f"Error creating database: {e}")
    finally:
        conn.close()

def save_data_to_db(data):
    if data is None:
        logging.error("No data to save to database.")
        return

    try:
        conn = sqlite3.connect('bart_schedule.db')
        cursor = conn.cursor()
        
        trips = data['root']['schedule']['request']['trip']
        
        for trip in trips:
            origin = trip['@origin']
            destination = trip['@destination']
            fare = float(trip['@fare'])
            orig_time = f"{trip['@origTimeDate']} {trip['@origTimeMin']}"
            dest_time = f"{trip['@destTimeDate']} {trip['@destTimeMin']}"
            trip_time = int(trip['@tripTime'])
            date = datetime.strptime(trip['@origTimeDate'], '%m/%d/%Y').strftime('%Y-%m-%d')
            
            cursor.execute('''INSERT INTO schedule (origin, destination, fare, orig_time, dest_time, trip_time, date)
                              VALUES (?, ?, ?, ?, ?, ?, ?)''', 
                              (origin, destination, fare, orig_time, dest_time, trip_time, date))
        
        conn.commit()
        logging.info("Data saved to database successfully.")
    except sqlite3.Error as e:
        logging.error(f"Error saving data to database: {e}")
    finally:
        conn.close()

def main():
    create_database()
    data = fetch_bart_data()
    save_data_to_db(data)

if __name__ == "__main__":
    main()