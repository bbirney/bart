import requests
import datetime
import time
import os
import json

# Function to fetch BART schedule
def fetch_bart_schedule():
    # BART API endpoint
    api_url = "http://api.bart.gov/api/sched.aspx"
    
    # Get the BART API key from the environment variable
    api_key = os.getenv('BART_API_KEY')
    if not api_key:
        print("BART_API_KEY environment variable not set")
        return
    
    # Parameters for the API request
    params = {
        'cmd': 'stnsched',
        'orig': 'ALL',  # Get schedule for all stations
        'date': 'today',
        'key': api_key,
        'json': 'y'
    }
    
    # Make the API request
    response = requests.get(api_url, params=params)
    
    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        schedule_data = response.json()
        
        # Save the schedule data to a file
        filename = f"bart_schedule_{datetime.date.today()}.json"
        with open(filename, 'w') as f:
            json.dump(schedule_data, f, indent=4)
        
        print(f"Schedule data saved to {filename}")
    else:
        print("Failed to fetch schedule data")

# Function to run the script at 1 AM daily
def schedule_daily_task():
    while True:
        now = datetime.datetime.now()
        target_time = now.replace(hour=1, minute=0, second=0, microsecond=0)
        
        # Calculate the time to sleep
        sleep_time = (target_time - now).total_seconds()
        
        # If the target time has already passed today, schedule for tomorrow
        if sleep_time < 0:
            sleep_time += 86400  # Seconds in a day
        
        print(f"Sleeping for {sleep_time} seconds")
        time.sleep(sleep_time)
        
        fetch_bart_schedule()

if __name__ == "__main__":
    schedule_daily_task()
