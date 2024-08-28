import appdaemon.plugins.hass.hassapi as hass
import requests
import statistics
import datetime
import json
import os
import math
import collections
import time
import traceback
import sqlite3
from dateutil.relativedelta import relativedelta
import urllib.parse
from dateutil import parser
from zoneinfo import ZoneInfo


def set_sensor_state(hass, entity_id, state, attributes=None):
    """
    Set the state and attributes of a sensor in Home Assistant.
    
    Args:
    hass: The Home Assistant instance
    entity_id: The ID of the sensor to update
    state: The new state value for the sensor
    attributes: Optional dictionary of attributes to set for the sensor
    """
    base_attributes = {
        "state_class": "measurement",
    }
    if attributes:
        base_attributes.update(attributes)
    hass.set_state(entity_id, state=state, attributes=base_attributes)

class SoCEstimator(hass.Hass):
    """
    A class to estimate the State of Charge (SoC) of a battery system with solar power integration.
    This class provides various methods to calculate, update, and predict battery charge levels
    based on solar production forecasts and historical data.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the SoCEstimator with default values and empty placeholders for various attributes.
        """
        super().__init__(*args, **kwargs)
        self.time_zone = None  # Time Zone is set in the initialize method by the timezone set in Appdaemon.yaml.
        self.last_known_coordinates = None  # Last known coordinates are set in the initialize method by the check_coordinates method. Leave this as None.
        self.last_significant_movement_time = None  # Last significant movement time is set in the initialize method by the check_coordinates method.  Leave this as None.
        self.current_location_name = None  # Current location name is set in the initialize method by the check_coordinates method. Leave this as None.
        self.battery_full_threshold = 99  # Percent state of charge at which the battery is considered full. Script may do weird things if the battery never reaches exactly this value; so 98/99 is usually preferred over 100 to account for small variations and glitches in your battery monitoring system.
        self.significant_movement_distance = 0.5  # in kilometers | Distance in kilometers at which the vehicle is considered to have moved.
        self.coordinate_check_interval = 30 * 60  # Check coordinates every 30 minutes (in seconds). Used to detect if the vehicle has moved significantly and update the solar delta calculation accordingly.
        self.new_location_stability_hours = 8  # Hours at which the vehicle is considered to have arrived at a new location. This prevents the script from creating new locations while in-transit.
        self.max_calculation_days = 30  # Maximum number of days to calculate the solar forecast delta for.
        self.shore_power_voltage_threshold = 100  # Voltage at which the vehicle is considered to be on shore power. 
        self.min_data_points_for_iqm = 4  # Minimum number of data points to use for the interquartile mean.     
        self.api_request_timeout = 10  # in seconds | Timeout for API requests.
        self.data_retention_period = datetime.timedelta(hours=24)  # How long to retain load data for the calculated weighted load average. You can expiriment and adjust this to improve accuracy.
        self.reverse_geocode_user_agent = 'SoCEstimator/0.8'  # User agent for reverse geocoding requests.
        self.solar_forecast_db = "/config/apps/storage/solar_forecast_data.db"  # Database for storing solar forecast data.  
        self.persistent_data_file = "/config/apps/storage/soc_estimator_data.json"  # File for storing persistent data.  


    def initialize(self):
        """
        Set up the SoCEstimator with initial configurations, schedule regular updates,
        and create necessary sensors in Home Assistant.
        """
        # Set up battery and solar system parameters
        self.battery_capacity_ah = 200  # Battery capacity in Amp hours. 
        self.nominal_voltage = 12.8  # Nominal battery bank voltage. 
        self.solar_capacity_kw = 0.4  # Solar capacity in kilowatts.
        self.hass_ip = "HOMEASSISTANT-IP"  # Home Assistant IP address or hostname. 
        self.hass_port = 8123 # Home Assistant port. 
        self.access_token = "YOUR-LONG-LIVED-ACCESS-TOKEN" # Home Assistant long-lived access token. 
        self.api_data_file = "/config/apps/storage/solar_forecast_data.json"  # File for storing solar forecast data.
        self.soc_adjustment_threshold = self.args.get("soc_adjustment_threshold", 97) # The threshold at which the script will consider the SOC valid for solar delta schema calculations. At a high SOC, the batteries will accept less current which will throw off the solar delta calculations.
             
        # Define sensors used by the estimator
        self.sensors = {
            # Input sensors (set and provide these for the script to work)  
            "state_of_charge": "sensor.battery_percent", # State of charge sensor. Expects a percent value (0-100).
            "gps_latitude": "sensor.gps_latitude", # GPS latitude sensor. Expects a float value.
            "gps_longitude": "sensor.gps_longitude", # GPS longitude sensor. Expects a float value.
            "dc_loads": "sensor.dc_loads", # DC loads sensor. Expects a float value. Real-time value in watts. Loads only (not solar production).
            "ac_volts": "sensor.watchdog_voltage_line_1", # AC Voltage sensor. Expects a float value. Real-time value in volts. Used to detect if the vehicle is on shore power. Optional, can be omitted.
            "current_solar_production": "sensor.current_solar_production", # Current solar production sensor. Real-time value in watts.
            # Output sensors (the script will create these; but you can override the names if you want to)
            "average_load": "sensor.average_load", # Average load sensor. Creates a new sensor with the average load over the last 24 hours as calculated by the script. Not needed; but gives you the ability to add it to your dashboard.
            "calculated_energy_production_today_remaining": "sensor.calculated_energy_production_today_remaining", # Calculated energy production today remaining sensor. Expects a float value. Real-time value in watts.
            "calculated_energy_production_tomorrow": "sensor.calculated_energy_production_tomorrow", # Calculated energy production tomorrow sensor. Real-time value in watts.
            "time_until_charged": "sensor.time_until_charged", # Time until charged sensor. Outputs similar to other Home Assistant time sensors. i.e., "In 3 hours"
            "solar_production_delta": "sensor.solar_production_delta", # Solar production delta sensor. Tells you how much the forecast is being adjusted to account for historic actual data.  
            "expected_peak_soc_today": "sensor.expected_peak_soc_today", # Expected peak SOC today sensor. Outputs a percentage value (0-100). Shows the expected peak SOC for today based on the solar forecast.
            "expected_peak_soc_tomorrow": "sensor.expected_peak_soc_tomorrow", # Expected peak SOC tomorrow sensor. Outputs a percentage value (0-100). Shows the expected peak SOC for tomorrow based on the solar forecast.
            "expected_minimum_soc": "sensor.expected_minimum_soc", # Expected minimum SOC sensor. Outputs a percentage value (0-100). Shows the expected minimum SOC for the next 24 hours based on the solar forecast.
            "time_to_minimum_soc": "sensor.time_to_minimum_soc", # Time to minimum SOC sensor. Outputs similar to other Home Assistant time sensors. i.e., "In 3 hours"
            "charged_time": "sensor.charged_time" # Charged time sensor. Outputs the exact time the batteries are expected to be fully charged. i.e., "Tomorow 11:34AM"
        }
        self.update_interval = 30*60 # How often to update the sensors in seconds.
        self.api_update_interval = 60*60 # How often to pull the latest solar forecast data from the API in seconds. NOTE: This API is free, but has a rate limit. It's the same API used by the Home Assistant Energy Dashboard; so if you're using that, consider the impact on the rate limit.
    
        # Initialize data structures for load tracking and forecasting
        self.load_data = collections.deque(maxlen=None) 
        self.data_retention_period = self.data_retention_period
        self.last_load_update = 0
        self.solar_forecast_data = {}
        self.average_load = 0  # Initialize average_load attribute
    
        # Add persistent storage for last known average load and high voltage time
        self.last_known_average_load = None
        self.last_high_voltage_time = None
    
        # Load persistent data if available
        self.load_persistent_data()
    
        # Load existing data from the database first
        self.load_existing_forecast_data()
    
        # Store handles to scheduled callbacks
        self.scheduled_callbacks = []

        # Schedule solar forecast updates
        self.log("Scheduling solar forecast data updates", level="DEBUG")
        self.scheduled_callbacks.append(self.run_every(self.update_solar_forecast, "now+5", self.api_update_interval))
    
        # Schedule regular load data updates
        self.scheduled_callbacks.append(self.run_every(self.update_load_data, "now", self.update_interval))
    
        # Create the new sensors in Home Assistant
        set_sensor_state(self, self.sensors["calculated_energy_production_today_remaining"], 0, 
                         {"friendly_name": "Calculated Energy Production Today Remaining",
                          "unit_of_measurement": "kWh", 
                          "icon": "mdi:solar-power"})
        set_sensor_state(self, self.sensors["calculated_energy_production_tomorrow"], 0, 
                         {"friendly_name": "Calculated Energy Production Tomorrow",
                          "unit_of_measurement": "kWh", 
                          "icon": "mdi:solar-power"})
        set_sensor_state(self, self.sensors["time_until_charged"], "Unknown", 
                         {"friendly_name": "Time Until Charged",
                          "icon": "mdi:battery-unknown"})
        
        # Set up the database for storing solar forecast and location data
        self.setup_database()
        self.setup_locations_table()
        self.log("SoCEstimator initialized successfully", level="DEBUG")

        # Create and set up the solar delta calculation switch
        if self.get_state("switch.solar_delta_calc") is None:
            self.set_state("switch.solar_delta_calc", state="off", attributes={
                "friendly_name": "Solar Delta Calculation",
                "icon": "mdi:solar-power"
            })

        # Listen for changes in the solar delta calculation switch
        self.listen_state(self.handle_solar_delta_calc_change, "switch.solar_delta_calc")

        # Log the current state of the solar delta calculation switch
        current_state = self.get_state("switch.solar_delta_calc")
        self.log(f"Current state of solar_delta_calc: {current_state}", level="DEBUG")
        self.handle_solar_delta_calc_change("switch.solar_delta_calc", "state", None, current_state, None)

        # Register the toggle service for the solar delta calculation
        self.register_service("soc_estimator/toggle_solar_delta_calc", self.toggle_solar_delta_calc)

        # Set up time zone
        self.time_zone = self.get_persistent_data("time_zone")
        if self.time_zone is None:
            self.time_zone = self.get_timezone()
            self.set_persistent_data("time_zone", self.time_zone)
        self.log(f"Script initialized with time zone: {self.time_zone}", level="INFO")

        # Set up battery icons for different charge levels
        self.battery_icons = {
            99: "mdi:battery", 90: "mdi:battery-90", 80: "mdi:battery-80",
            70: "mdi:battery-70", 60: "mdi:battery-60", 50: "mdi:battery-50",
            40: "mdi:battery-40", 30: "mdi:battery-30", 20: "mdi:battery-20",
            10: "mdi:battery-10", 0: "mdi:battery-outline"
        }

        # Load last known coordinates and significant movement time from persistent storage
        self.last_known_coordinates = self.get_persistent_data("last_known_coordinates")
        self.last_significant_movement_time = self.get_persistent_data("last_significant_movement_time")

        # Schedule coordinate check every 30 minutes
        self.run_every(self.check_coordinates, "now", self.coordinate_check_interval)
        self.log(f"SoC adjustment threshold set to: {self.soc_adjustment_threshold}%", level="DEBUG")    


    @staticmethod
    def interquartile_mean(data):
        """
        Calculate the interquartile mean of a dataset.
        This method is used to get a robust average that's less affected by outliers.
        
        Args:
        data: A list of numerical values
        
        Returns:
        The interquartile mean of the data, or the regular mean if there are fewer than 4 data points
        """
        if len(data) < 4:  # Not enough data for meaningful quartiles
            return sum(data) / len(data)
        sorted_data = sorted(data)
        q1, q3 = len(sorted_data) // 4, 3 * len(sorted_data) // 4
        return statistics.mean(sorted_data[q1:q3+1])

    def check_coordinates(self, kwargs):
        """
        Check if the vehicle has moved significantly and update the solar delta calculation accordingly.
        This method is called periodically to detect changes in location and manage the solar delta calculation.
        
        Args:
        kwargs: Additional keyword arguments (not used in this method)
        """
        try:
            current_lat = float(self.get_state(self.sensors["gps_latitude"]))
            current_lon = float(self.get_state(self.sensors["gps_longitude"]))
            current_coordinates = (current_lat, current_lon)
            current_time = datetime.datetime.now(ZoneInfo(self.time_zone))

            if self.last_known_coordinates is None:
                self.last_known_coordinates = current_coordinates
                self.set_persistent_data("last_known_coordinates", current_coordinates)
                self.set_arrival_time_at_current_location(current_time)
                return

            distance = self.haversine(self.last_known_coordinates, current_coordinates)

            if distance > self.significant_movement_distance:  # 0.5km is default distance for significant movement. This can be configured in the initialize method.   
                self.log(f"Vehicle moved more than {self.significant_movement_distance} km. Distance: {distance:.2f} km", level="INFO")
                self.set_state("switch.solar_delta_calc", state="off")
                self.last_significant_movement_time = current_time.isoformat()
                self.set_persistent_data("last_significant_movement_time", self.last_significant_movement_time)
                self.set_persistent_data("last_known_coordinates", current_coordinates)
                self.last_known_coordinates = current_coordinates
                self.current_location_name = None
                self.set_persistent_data("first_arrival_time", None)  # Reset arrival time
                self.set_arrival_time_at_current_location(current_time)  # Set new arrival time
            else:
                nearby_location = self.check_nearby_locations(current_coordinates)
                if nearby_location:
                    self.current_location_name = nearby_location
                    self.log(f"Vehicle is at known location: {nearby_location}", level="INFO")
                    self.set_state("switch.solar_delta_calc", state="on")
                    
                    # Check if arrival time is set, if not, set it to now
                    if self.get_arrival_time_at_current_location() is None:
                        self.log("Arrival time not set for current location. Setting it to now.", level="INFO")
                        self.set_arrival_time_at_current_location(current_time)
                else:
                    self.check_and_create_new_location(current_lat, current_lon, current_time)

        except Exception as e:
            self.log(f"Error in check_coordinates: {e}", level="ERROR")

    def check_and_create_new_location(self, lat, lon, current_time):
        """
        Check if the current location is a new, stable location and create a new entry if necessary.
        This method is used to identify and record new locations where the vehicle stays for extended periods.
        
        Args:
        lat: The current latitude
        lon: The current longitude
        current_time: The current timestamp
        """
        first_arrival_time = self.get_persistent_data("first_arrival_time")
        
        if first_arrival_time is None:
            self.set_persistent_data("first_arrival_time", current_time.isoformat())
            self.log("Started tracking new potential location", level="INFO")
        else:
            first_arrival_time = datetime.datetime.fromisoformat(first_arrival_time)
            time_difference = current_time - first_arrival_time
            
            if time_difference >= datetime.timedelta(hours=self.new_location_stability_hours):
                location_name = self.reverse_geocode(lat, lon)
                self.add_new_location(location_name, lat, lon)
                self.current_location_name = location_name
                self.log(f"New location added after {self.new_location_stability_hours} hours: {location_name}", level="INFO")
                self.set_state("switch.solar_delta_calc", state="on")
                self.set_persistent_data("first_arrival_time", None)  # Reset after adding
            else:
                self.log(f"Waiting for {self.new_location_stability_hours} hour stability. Time passed: {time_difference}", level="DEBUG")

    def add_new_location(self, name, lat, lon):
        """
        Add a new location to the database.
        This method is called when a new stable location is identified.
        
        Args:
        name: The name of the new location
        lat: The latitude of the new location
        lon: The longitude of the new location
        """
        conn = sqlite3.connect(self.solar_forecast_db)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO locations (name, latitude, longitude)
            VALUES (?, ?, ?)
        ''', (name, lat, lon))
        conn.commit()
        conn.close()
        self.log(f"Added new location to database: {name} ({lat}, {lon})", level="INFO")

    def check_nearby_locations(self, current_coordinates):
        """
        Check if the current coordinates are near any known locations in the database.
        
        Args:
        current_coordinates: A tuple of (latitude, longitude) representing the current position
        
        Returns:
        The name of the nearby location if found, otherwise None
        """
        conn = sqlite3.connect(self.solar_forecast_db)
        cursor = conn.cursor()
        cursor.execute('SELECT name, latitude, longitude FROM locations')
        locations = cursor.fetchall()
        conn.close()

        for name, lat, lon in locations:
            distance = self.haversine(current_coordinates, (lat, lon))
            if distance <= self.significant_movement_distance:  # Within 500 meters
                return name
        return None

    def calculate_updated_schema(self, base_schema):
        # Get the arrival time at the current location
        arrival_time = self.get_arrival_time_at_current_location()
        
        # If no arrival time is set, return the base schema without modifications
        if arrival_time is None:
            return base_schema

        # Convert the arrival time string to a datetime object
        arrival_time = datetime.datetime.fromisoformat(arrival_time)
        
        # Calculate the number of days spent at the current location
        days_at_location = (datetime.datetime.now(ZoneInfo(self.time_zone)) - arrival_time).days

        # If less than a day has passed, return the base schema without modifications
        if days_at_location < 1:
            return base_schema

        # Calculate and return an updated schema based on the time spent at the location
        return self.calculate_schema_for_period(arrival_time, base_schema)

    def calculate_new_schema(self, arrival_time):
        # Get the arrival time at the current location
        if arrival_time is None:
            # If no arrival time is set, use 30 days ago as the start time
            self.log("Arrival time not set. Using 30 days ago as start time.", level="WARNING")
            arrival_time = datetime.datetime.now(ZoneInfo(self.time_zone)) - datetime.timedelta(days=30)

        # Convert the arrival time string to a datetime object if it's not already
        if isinstance(arrival_time, str):
            arrival_time = datetime.datetime.fromisoformat(arrival_time)

        # Calculate and return a new schema based on the arrival time
        return self.calculate_schema_for_period(arrival_time)

    def calculate_schema_for_period(self, start_time, base_schema=None):
        end_time = datetime.datetime.now(ZoneInfo(self.time_zone))
        self.log(f"Calculating schema from {start_time} to {end_time}", level="DEBUG")
        
        # Limit the calculation period to a maximum of 30 days
        max_days = self.max_calculation_days
        start_time = max(start_time, end_time - datetime.timedelta(days=max_days))

        # Retrieve solar forecasts, actual productions, and SoC data for the date range
        all_forecasts = self.get_solar_forecasts_for_date_range(start_time.date(), end_time.date())
        all_productions = self.get_actual_productions_for_date_range(start_time.date(), end_time.date())
        all_soc_data = self.get_historical_soc_data_range(start_time.date(), end_time.date())

        # Initialize a dictionary to store adjustment factors for each hour
        adjustment_factors = {hour: [] for hour in range(24)}

        current_date = start_time.date()
        while current_date <= end_time.date():
            day_forecast = all_forecasts.get(current_date, {})
            day_production = all_productions.get(current_date, [])

            # Determine the maximum hour to process for this day
            if current_date == end_time.date():
                max_hour = end_time.hour - 1  # Exclude the current hour
            else:
                max_hour = 23

            for hour in range(max_hour + 1):  # +1 here because range is exclusive of the upper bound
                hour_start = self.ensure_timezone_aware(datetime.datetime.combine(current_date, datetime.time(hour, 0)))
                
                # This check is now redundant, but we can keep it for extra safety
                if hour_start >= end_time.replace(minute=0, second=0, microsecond=0):
                    continue

                hour_end = hour_start + datetime.timedelta(hours=1)

                is_valid, max_soc = self.is_soc_valid_for_hour_cached(all_soc_data, hour_start, hour_end)
                forecast_wh = self.get_forecast_wh_for_hour(day_forecast, hour_start, hour_end)
                actual_wh = self.calculate_actual_wh_for_hour(day_production, hour_start, hour_end)

                self.log(f"Date: {current_date}, Hour {hour}: is_valid={is_valid}, max_soc={max_soc}, forecast_wh={forecast_wh}, actual_wh={actual_wh}", level="DEBUG")

                if is_valid and forecast_wh > 0:
                    adjustment_factor = actual_wh / forecast_wh
                    adjustment_factors[hour].append(adjustment_factor)

            current_date += datetime.timedelta(days=1)

        # Calculate the new schema based on the collected adjustment factors
        new_schema = {}
        for hour, factors in adjustment_factors.items():
            if factors:
                # If we have adjustment factors, use their average
                new_schema[hour] = sum(factors) / len(factors)
            elif base_schema:
                # If no factors but we have a base schema, use the base schema value
                new_schema[hour] = base_schema.get(hour, 1.0)
            else:
                # If no factors and no base schema, use a default value of 1.0
                new_schema[hour] = 1.0

        # Log the new schema and data points per hour for debugging
        self.log(f"New schema calculated: {new_schema}", level="DEBUG")
        self.log(f"Data points per hour: {[len(factors) for factors in adjustment_factors.values()]}", level="DEBUG")

        # If we have a current location name, save the schema for this location
        if self.current_location_name:
            self.save_location_schema(self.current_location_name, new_schema)

        # Set the solar production delta based on the new schema
        self.set_solar_production_delta(new_schema)
        
        return new_schema

    def ensure_timezone_aware(self, dt):
        # If the datetime is not timezone aware, make it aware
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            if self.time_zone is None:
                # If time_zone is not set, use UTC and log a warning
                self.log("Time zone not yet initialized, using UTC", level="WARNING")
                return dt.replace(tzinfo=datetime.timezone.utc)
            # Make the datetime aware using the instance's time zone
            return dt.replace(tzinfo=ZoneInfo(self.time_zone))
        # If already timezone aware, return as is
        return dt

    def handle_rate_limiting(self, headers, response_json):
        retry_at = None
        # Check if rate limit info is in the response JSON
        if 'ratelimit' in response_json.get('message', {}):
            retry_at = response_json['message']['ratelimit'].get('retry-at')
        # If not in JSON, check if it's in the headers
        elif 'Retry-After' in headers:
            retry_at = headers['Retry-After']
        
        if retry_at:
            # Convert retry time to timezone aware datetime
            retry_time = self.ensure_timezone_aware(datetime.datetime.fromisoformat(retry_at.replace('Z', '+00:00')))
            # Store the retry time in persistent data
            self.set_persistent_data('solar_api_retry_time', retry_time.isoformat())
            self.log(f"API rate limit reached. Next retry at: {retry_time}", level="ERROR")
        
        # Store the complete rate limit info in persistent storage
        rate_limit_info = {
            'retry_at': retry_at,
            'zone': response_json.get('message', {}).get('ratelimit', {}).get('zone'),
            'period': response_json.get('message', {}).get('ratelimit', {}).get('period'),
            'limit': response_json.get('message', {}).get('ratelimit', {}).get('limit')
        }
        self.set_persistent_data('solar_api_rate_limit_info', rate_limit_info)

    def load_existing_forecast_data(self):
        try:
            # Connect to the SQLite database
            conn = sqlite3.connect(self.solar_forecast_db)
            cursor = conn.cursor()
            
            # Fetch all forecast data from the database
            cursor.execute('SELECT timestamp, watt_hours FROM solar_forecast')
            
            # Store the fetched data in the instance variable
            self.solar_forecast_data = {row[0]: row[1] for row in cursor.fetchall()}
            
            conn.close()
            
            # Log the number of entries loaded and a sample of the data
            self.log(f"Loaded existing solar forecast data: {len(self.solar_forecast_data)} entries", level="DEBUG")
            self.log(f"Sample data: {dict(list(self.solar_forecast_data.items())[:5])}", level="DEBUG")
        except Exception as e:
            # Log any errors that occur during the process
            self.log(f"Error loading existing forecast data: {e}", level="ERROR")
            self.solar_forecast_data = {}

    def fetch_data_from_api(self, url, headers=None):
        try:
            # Send GET request to the API
            response = requests.get(url, headers=headers, timeout=self.api_request_timeout)
            response_json = response.json()

            # Check if the response indicates rate limiting (HTTP 429)
            if response.status_code == 429:  # Too Many Requests
                self.handle_rate_limiting(response.headers, response_json)
                return None

            # Raise an exception for any other HTTP errors
            response.raise_for_status()
            return response_json
        except requests.exceptions.RequestException as e:
            # Log any request-related errors
            self.log(f"Error fetching data from API: {e}", level="ERROR")
            return None

    def toggle_solar_delta_calc(self, kwargs):
        # Get the current state of the solar delta calculation switch
        current_state = self.get_state("switch.solar_delta_calc")
        
        # Toggle the state
        new_state = "on" if current_state == "off" else "off"
        
        # Set the new state
        self.set_state("switch.solar_delta_calc", state=new_state)
        
        # Log the state change
        self.log(f"Toggled solar_delta_calc from {current_state} to {new_state}", level="DEBUG")
        
        # Handle the state change
        self.handle_solar_delta_calc_change("switch.solar_delta_calc", "state", current_state, new_state, None)

    def handle_solar_delta_calc_change(self, entity, attribute, old, new, kwargs):
        # Log the detected change
        self.log(f"Solar delta calculation change detected. Old: {old}, New: {new}", level="DEBUG")
        
        if new == "on":
            # If turned on, update forecast with adjustments
            self.log("Solar delta calculation enabled. Updating forecast with adjustments.", level="DEBUG")
            self.update_solar_forecast({'force_update': True})
        elif new == "off":
            # If turned off, revert to unadjusted forecast
            self.log("Solar delta calculation disabled. Reverting to unadjusted forecast.", level="DEBUG")
            self.update_solar_forecast({'force_update': True})
        else:
            # Log a warning for unexpected states
            self.log(f"Unexpected state for solar_delta_calc: {new}", level="WARNING")

    def setup_database(self):
        # Connect to the SQLite database
        conn = sqlite3.connect(self.solar_forecast_db)
        cursor = conn.cursor()
        
        # Create the solar_forecast table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS solar_forecast (
                timestamp TEXT PRIMARY KEY,
                watt_hours REAL,
                last_updated TEXT
            )
        ''')
        
        # Commit the changes and close the connection
        conn.commit()
        conn.close()

    def setup_locations_table(self):
        # Connect to the SQLite database
        conn = sqlite3.connect(self.solar_forecast_db)
        cursor = conn.cursor()
        
        # Create the locations table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS locations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE,
                latitude REAL,
                longitude REAL,
                schema TEXT
            )
        ''')
        
        # Commit the changes and close the connection
        conn.commit()
        conn.close()

    def is_solar_delta_calc_enabled(self):
        # Get the current state of the solar delta calculation switch
        state = self.get_state("switch.solar_delta_calc")
        
        # Log the current state
        self.log(f"Current state of solar_delta_calc: {state}", level="DEBUG")
        
        # Return True if the state is "on", False otherwise
        return state == "on"

    def get_local_utc_offset(self):
        # Get the current local time and UTC time
        now = datetime.datetime.now()
        utc_now = datetime.datetime.utcnow()
        
        # Calculate and return the offset in hours
        return round((now - utc_now).total_seconds() / 3600)
    
    def load_persistent_data(self):
        try:
            # Attempt to open and read the persistent data file
            with open(self.persistent_data_file, 'r') as f:
                data = json.load(f)
                # Load specific data points
                self.last_known_average_load = data.get('last_known_average_load')
                self.last_high_voltage_time = data.get('last_high_voltage_time')
        except FileNotFoundError:
            # Log if the file doesn't exist
            self.log("No persistent data file found. Creating a new one.", level="DEBUG")
        except json.JSONDecodeError:
            # Log if there's an error decoding the JSON
            self.log("Error decoding persistent data file. Starting with empty data.", level="ERROR")
    
    def set_persistent_data(self, key, value):
        try:
            # Try to read existing data
            with open(self.persistent_data_file, 'r') as f:
                data = json.load(f)
        except FileNotFoundError:
            # If file doesn't exist, start with an empty dictionary
            data = {}
        except json.JSONDecodeError:
            # If there's an error decoding the JSON, log it and start with an empty dictionary
            self.log("Error decoding persistent data file. Starting with empty data.", level="ERROR")
            data = {}
        
        # Update the data with the new key-value pair
        data[key] = value
        
        # Write the updated data back to the file
        with open(self.persistent_data_file, 'w') as f:
            json.dump(data, f, indent=4)
    
    def get_persistent_data(self, key):
        try:
            # Attempt to open and read the persistent data file
            with open(self.persistent_data_file, 'r') as f:
                data = json.load(f)
            # Return the value for the given key
            return data.get(key)
        except FileNotFoundError:
            # Return None if the file doesn't exist
            return None
        except json.JSONDecodeError:
            # Log if there's an error decoding the JSON and return None
            self.log("Error decoding persistent data file.", level="ERROR")
            return None

    def update_forecast_database(self, new_forecast_data):
        # Connect to the SQLite database
        conn = sqlite3.connect(self.solar_forecast_db)
        cursor = conn.cursor()

        # Extract the local time from the API response
        last_updated = new_forecast_data.get('message', {}).get('info', {}).get('time')

        if last_updated:
            # Update or insert new forecast data
            for timestamp, value in new_forecast_data.get('result', {}).get('watt_hours', {}).items():
                cursor.execute('''
                    INSERT OR REPLACE INTO solar_forecast (timestamp, watt_hours, last_updated)
                    VALUES (?, ?, ?)
                ''', (timestamp, value, last_updated))

            # Delete old data (older than 60 days)
            cutoff_date = (datetime.datetime.now() - datetime.timedelta(days=60)).strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute('DELETE FROM solar_forecast WHERE timestamp < ?', (cutoff_date,))

            # Commit changes and update the instance variable
            conn.commit()
            cursor.execute('SELECT timestamp, watt_hours FROM solar_forecast')
            self.solar_forecast_data = {row[0]: row[1] for row in cursor.fetchall()}
            conn.close()
            
            self.log("Updated solar forecast database", level="DEBUG")
            self.log(f"New forecast data: {dict(list(self.solar_forecast_data.items())[:5])}", level="DEBUG")
        else:
            self.log("No valid timestamp found in API response", level="WARNING")
            conn.close()

    def get_last_valid_gps_coordinates(self):
        """
        Retrieve the most recent valid GPS coordinates from historical data.

        Returns:
            tuple: (latitude, longitude) or (None, None) if no valid coordinates found
        """
        try:
            # Get the current time
            now = self.ensure_timezone_aware(datetime.datetime.now())
            # Set the start time to 7 days ago
            start_time = now - datetime.timedelta(days=7)

            # Fetch historical data for latitude and longitude
            lat_data = self.get_historical_sensor_data(self.sensors["gps_latitude"], start_time, now)
            lon_data = self.get_historical_sensor_data(self.sensors["gps_longitude"], start_time, now)

            # Find the most recent valid latitude and longitude
            valid_lat = next((float(entry['state']) for entry in reversed(lat_data) 
                              if entry['state'] not in ['unknown', 'unavailable']), None)
            valid_lon = next((float(entry['state']) for entry in reversed(lon_data) 
                              if entry['state'] not in ['unknown', 'unavailable']), None)

            if valid_lat is not None and valid_lon is not None:
                self.log(f"Found last valid GPS coordinates: {valid_lat}, {valid_lon}", level="DEBUG")
                return valid_lat, valid_lon
            else:
                self.log("No valid GPS coordinates found in the last 7 days", level="WARNING")
                return None, None

        except Exception as e:
            self.log(f"Error in get_last_valid_gps_coordinates: {e}", level="ERROR")
            return None, None

    def get_historical_sensor_data(self, entity_id, start_time, end_time):
        """
        Retrieve historical data for a specific sensor.

        Args:
            entity_id (str): The entity ID of the sensor
            start_time (datetime): Start of the time range
            end_time (datetime): End of the time range

        Returns:
            list: List of historical state entries for the sensor
        """
        try:
            # Format times in ISO 8601 format
            start_time_str = start_time.isoformat()
            end_time_str = end_time.isoformat()
            
            # URL encode the parameters
            entity_id_encoded = urllib.parse.quote(entity_id)
            start_time_encoded = urllib.parse.quote(start_time_str)
            end_time_encoded = urllib.parse.quote(end_time_str)
            
            # Construct the URL for the Home Assistant API
            url = f"http://{self.hass_ip}:{self.hass_port}/api/history/period/{start_time_encoded}?filter_entity_id={entity_id_encoded}&end_time={end_time_encoded}"
            
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json"
            }
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            if data and isinstance(data, list) and len(data) > 0:
                return data[0]
            else:
                self.log(f"No historical data available for {entity_id}", level="WARNING")
                return []
        except Exception as e:
            self.log(f"Error retrieving historical data for {entity_id}: {e}", level="ERROR")
            return []

    def update_solar_forecast(self, kwargs=None):
        try:
            # Check if time zone is initialized
            if self.time_zone is None:
                self.log("Time zone not yet initialized, deferring update", level="DEBUG")
                return
            
            # Get current time and force update flag
            current_time = self.ensure_timezone_aware(datetime.datetime.now())
            force_update = kwargs.get('force_update', False) if kwargs else False

            self.log(f"Entering update_solar_forecast. Force update: {force_update}", level="DEBUG")

            # Determine if update is needed
            update_needed = force_update
            if not force_update:
                # Check if we have recent data
                last_updated = self.get_persistent_data('last_forecast_update')
                if last_updated:
                    last_updated = self.ensure_timezone_aware(datetime.datetime.fromisoformat(last_updated))
                    time_since_update = current_time - last_updated
                    update_needed = time_since_update >= datetime.timedelta(seconds=self.api_update_interval)
                else:
                    update_needed = True

            # Check for rate limiting
            if update_needed:
                retry_time = self.get_persistent_data('solar_api_retry_time')
                if retry_time:
                    retry_time = self.ensure_timezone_aware(datetime.datetime.fromisoformat(retry_time))
                    if current_time < retry_time:
                        self.log(f"API rate limited. Next retry at {retry_time}", level="DEBUG")
                        update_needed = False

            # Fetch new data if update is needed
            if update_needed:
                # Get current GPS coordinates
                latitude = self.get_state(self.sensors["gps_latitude"])
                longitude = self.get_state(self.sensors["gps_longitude"])

                # Check if current GPS coordinates are valid
                if latitude in ['unknown', 'unavailable'] or longitude in ['unknown', 'unavailable']:
                    self.log("Current GPS coordinates are invalid. Attempting to use last known valid coordinates.", level="WARNING")
                    latitude, longitude = self.get_last_valid_gps_coordinates()

                # If we still don't have valid coordinates, log an error and return
                if latitude is None or longitude is None:
                    self.log("Unable to obtain valid GPS coordinates. Solar forecast update aborted.", level="ERROR")
                    return

                url = f"https://api.forecast.solar/estimate/{latitude}/{longitude}/0/0/{self.solar_capacity_kw}"
                
                self.log(f"Calling solar forecast API: {url}", level="DEBUG")
                new_forecast_data = self.fetch_data_from_api(url)
                if new_forecast_data is not None:
                    # Update forecast database and last update time
                    self.update_forecast_database(new_forecast_data)
                    self.set_persistent_data('last_forecast_update', current_time.isoformat())
                    
                    # Extract and store timezone information from API response
                    api_timezone = new_forecast_data.get('message', {}).get('info', {}).get('timezone')
                    if api_timezone:
                        self.time_zone = api_timezone
                        self.set_persistent_data("time_zone", api_timezone)
                        self.log(f"Updated time zone from API: {api_timezone}", level="INFO")
                    
                    self.log("Successfully updated solar forecast data", level="DEBUG")
                else:
                    self.log("Failed to fetch new solar forecast data", level="WARNING")
            else:
                self.log("Using existing solar forecast data", level="DEBUG")

            # Calculate and apply adjustment schema
            self.log("Calculating solar adjustment schema", level="DEBUG")
            adjustment_schema = self.calculate_solar_adjustment_schema()
            self.log(f"Is solar delta calc enabled: {self.is_solar_delta_calc_enabled()}", level="DEBUG")
            if self.is_solar_delta_calc_enabled():
                self.log("Applying adjustment schema to solar forecast data", level="DEBUG")
                self.solar_forecast_data = self.apply_adjustment_schema(self.solar_forecast_data, adjustment_schema)
                self.log("Solar forecast data adjusted", level="DEBUG")
            else:
                self.log("Solar delta calculation is disabled, not applying adjustment", level="DEBUG")

        except Exception as e:
            # Log any errors that occur during the process
            self.log(f"Error in update_solar_forecast: {e}", level="ERROR")
            self.log(f"Error traceback: {traceback.format_exc()}", level="ERROR")

        # Always call calculate_soc, regardless of API success or failure
        self.calculate_soc()

    def get_state_with_retry(self, entity_id, retries=3, delay=2):
        """
        Attempt to get the state of an entity with retries.
        
        Args:
        entity_id (str): The ID of the entity to get the state for.
        retries (int): Number of retry attempts.
        delay (int): Delay in seconds between retries.
        
        Returns:
        The state of the entity or None if all retries fail.
        """
        for _ in range(retries):
            state = self.get_state(entity_id)
            if state is not None:
                return state
            self.sleep(delay)
        self.log(f"Failed to fetch state for {entity_id} after {retries} retries", level="ERROR")
        return None

    def calculate_weighted_average(self):
        """
        Calculate a weighted average of load data over the past 24 hours.
        
        Returns:
        float: The calculated weighted average load.
        """
        try:
            current_time = self.ensure_timezone_aware(datetime.datetime.now())
            window_start = current_time - datetime.timedelta(hours=24)  # 24-hour window
        
            # Adjust start time based on last high voltage time
            last_high_voltage_time = self.get_persistent_data("last_high_voltage_time")
            if last_high_voltage_time:
                last_high_voltage_time = self.ensure_timezone_aware(datetime.datetime.fromisoformat(last_high_voltage_time))
                start_time = max(window_start, last_high_voltage_time + datetime.timedelta(hours=1))
            else:
                start_time = window_start
        
            # Filter relevant data
            relevant_data = [(t, load) for t, load in self.load_data if t >= start_time]
        
            if not relevant_data:
                return self.get_persistent_data("last_known_average_load") or 0
        
            # Group data into hourly buckets
            hourly_buckets = collections.defaultdict(list)
            for timestamp, load in relevant_data:
                bucket = timestamp.replace(minute=0, second=0, microsecond=0)
                hourly_buckets[bucket].append(load)
        
            # Sort hourly buckets by timestamp
            sorted_buckets = sorted(hourly_buckets.items(), key=lambda x: x[0])
        
            # Initialize EMA with last known average load
            ema = self.get_persistent_data("last_known_average_load") or 0
        
            # Calculate EMA
            for bucket, loads in sorted_buckets:
                avg_load = sum(loads) / len(loads)
                hours_ago = (current_time - bucket).total_seconds() / 3600
                
                # Use faster decay for recent 8 hours, slower for older data
                k = 0.3 if hours_ago <= 8 else 0.1
                
                ema = (avg_load * k) + (ema * (1 - k))
        
            # Round the final EMA to two decimal places
            final_ema = round(ema, 2)
        
            # Store the new EMA as the last known average load
            self.set_persistent_data("last_known_average_load", final_ema)
        
            return final_ema
        
        except Exception as e:
            self.log(f"Error in calculate_weighted_average: {e}")
            return self.get_persistent_data("last_known_average_load") or 0

    def calculate_energy_production(self):
        """
        Calculate energy production for today and tomorrow based on solar forecast data.
        
        Returns:
        tuple: (energy_production_today_remaining, last_value_tomorrow)
        """
        local_tz = ZoneInfo(self.time_zone)
        now = self.ensure_timezone_aware(datetime.datetime.now())
        today = now.date()
        tomorrow = today + datetime.timedelta(days=1)

        # Sort the forecast data
        sorted_forecast = sorted(
            ((self.ensure_timezone_aware(parser.isoparse(ts)), wh) 
             for ts, wh in self.solar_forecast_data.items()),
            key=lambda x: x[0]
        )

        # Initialize variables
        last_value_today = 0
        last_value_tomorrow = 0
        current_forecast = 0

        # Find the last value for today and tomorrow
        for timestamp, watt_hours in sorted_forecast:
            if timestamp.date() == today:
                last_value_today = watt_hours / 1000  # Convert Wh to kWh
            elif timestamp.date() == tomorrow:
                last_value_tomorrow = watt_hours / 1000  # Convert Wh to kWh

        # Find the current forecast value
        for timestamp, watt_hours in sorted_forecast:
            if timestamp.date() == today and timestamp <= now:
                current_forecast = watt_hours / 1000  # Convert Wh to kWh
            elif timestamp > now:
                break

        # Calculate remaining energy production for today
        energy_production_today_remaining = max(0, last_value_today - current_forecast)

        # Log the values for debugging
        self.log(f"Current time: {now}", level="DEBUG")
        self.log(f"Last forecast value today: {last_value_today:.3f} kWh", level="DEBUG")
        self.log(f"Current forecast value: {current_forecast:.3f} kWh", level="DEBUG")
        self.log(f"Remaining forecast production today: {energy_production_today_remaining:.3f} kWh", level="DEBUG")
        self.log(f"Forecast production tomorrow: {last_value_tomorrow:.3f} kWh", level="DEBUG")

        # Update the sensors
        set_sensor_state(self, self.sensors["calculated_energy_production_today_remaining"], 
                         round(energy_production_today_remaining, 3),
                         {"unit_of_measurement": "kWh", "icon": "mdi:solar-power"})
        set_sensor_state(self, self.sensors["calculated_energy_production_tomorrow"], 
                         round(last_value_tomorrow, 3),
                         {"unit_of_measurement": "kWh", "icon": "mdi:solar-power"})

        return energy_production_today_remaining, last_value_tomorrow

    def calculate_soc(self):
        """
        Calculate and update various State of Charge (SoC) related metrics.
        """
        try:
            # Check if time zone is initialized
            if self.time_zone is None:
                self.log("Time zone not yet initialized, deferring calculation", level="DEBUG")
                return
            self.log("Starting calculate_soc", level="DEBUG")
            
            # Get current time and SoC
            current_time = self.ensure_timezone_aware(datetime.datetime.now())
            current_soc = float(self.get_state_with_retry(self.sensors["state_of_charge"]))
            
            # Calculate energy production
            energy_production_today, energy_production_tomorrow = self.calculate_energy_production()

            # Get total energy production for today
            total_energy_production_today = self.get_total_energy_production_today()

            self.log(f"Current SoC: {current_soc}%, Today's total production: {total_energy_production_today:.3f}kWh, Tomorrow's: {energy_production_tomorrow:.3f}kWh", level="DEBUG")

            # Check if on shore power
            ac_voltage = self.get_state(self.sensors["ac_volts"])
            is_on_shore_power = ac_voltage not in ['unavailable', 'unknown'] and float(ac_voltage) >= self.shore_power_voltage_threshold

            # Calculate average load
            self.average_load = self.calculate_weighted_average()
            calculation_load = 0 if is_on_shore_power else self.average_load

            self.log(f"Average load: {self.average_load}W, Calculation load: {calculation_load}W", level="DEBUG")

            # Calculate peak SoC for today and tomorrow
            peak_soc_today = self.calculate_peak_soc(current_soc, total_energy_production_today, calculation_load)
            peak_soc_tomorrow = self.calculate_peak_soc(current_soc, energy_production_tomorrow, calculation_load)

            self.log(f"Calculated peak SoC today: {peak_soc_today}%", level="DEBUG")
            self.log(f"Calculated peak SoC tomorrow: {peak_soc_tomorrow}%", level="DEBUG")

            # Apply the 100% display rule
            peak_soc_today_display = 100 if peak_soc_today >= self.battery_full_threshold else peak_soc_today
            peak_soc_tomorrow_display = 100 if peak_soc_tomorrow >= self.battery_full_threshold else peak_soc_tomorrow

            # Calculate minimum SoC
            min_soc, time_to_minimum_soc = self.calculate_minimum_soc(current_soc, calculation_load)

            # Format time_to_minimum_soc
            if time_to_minimum_soc:
                hours = time_to_minimum_soc.total_seconds() / 3600
                if hours >= 1:
                    time_str = f"In {round(hours)} hours"
                else:
                    minutes = round(hours * 60)
                    time_str = f"In {minutes} minutes"
            else:
                time_str = "Unknown"

            # Update sensors using a centralized method
            self.update_sensors({
                "average_load": round(self.average_load),
                "expected_peak_soc_today": round(peak_soc_today_display),
                "expected_peak_soc_tomorrow": round(peak_soc_tomorrow_display),
                "expected_minimum_soc": round(min_soc),
                "time_to_minimum_soc": time_str,
            })

            # Calculate charge time
            charge_time_icon, charge_time = self.calculate_charge_time(current_soc, self.average_load)

            # Update charge time sensors
            self.update_charge_time_sensors(charge_time_icon, charge_time, current_soc)

        except Exception as e:
            self.log(f"Error in calculate_soc: {e}", level="ERROR")
            self.log(f"Error traceback: {traceback.format_exc()}", level="ERROR")

    def update_sensors(self, sensor_data):
        """
        Update multiple sensors with their respective values and attributes.
        
        Args:
        sensor_data (dict): A dictionary of sensor names and their values to update.
        """
        for sensor, value in sensor_data.items():
            attributes = {}
            if sensor == "average_load":
                attributes = {"unit_of_measurement": "W"}
            elif sensor in ["expected_peak_soc_today", "expected_peak_soc_tomorrow", "expected_minimum_soc"]:
                attributes = {
                    "icon": self.get_battery_icon(value),
                    "unit_of_measurement": "%"
                }
            elif sensor == "time_to_minimum_soc":
                attributes = {"icon": "mdi:clock-alert"}
            
            set_sensor_state(self, f"sensor.{sensor}", value, attributes)

    def update_charge_time_sensors(self, charge_time_icon, charge_time, current_soc):
        """
        Update sensors related to battery charge time.
        
        Args:
        charge_time_icon (str): Icon to use for the charge time sensor.
        charge_time (datetime): Estimated time when the battery will be fully charged.
        current_soc (float): Current State of Charge of the battery.
        """
        if current_soc >= self.battery_full_threshold:
            time_until_charged = "Fully Charged"
            charged_time = "Fully Charged"
            icon = "mdi:battery-charging-100"
        elif charge_time is None:
            time_until_charged = "Unknown"
            charged_time = "Unknown"
            icon = "mdi:battery-unknown"
        else:
            # Calculate the time until the battery is fully charged
            time_until_full = (charge_time - self.ensure_timezone_aware(datetime.datetime.now())).total_seconds() / 3600
            
            # Format the time until charged based on the duration
            if time_until_full < 1:
                # If less than an hour, display in minutes
                time_until_charged = f"In {int(time_until_full * 60)} minutes"
            else:
                # If an hour or more, display in hours (rounded)
                time_until_charged = f"In {round(time_until_full)} hours"
            
            # Format the charged time based on whether it's today or tomorrow
            if charge_time.date() == datetime.datetime.now().date():
                # If it's today, just show the time
                charged_time = charge_time.strftime("%I:%M%p")
            else:
                # If it's tomorrow, include "Tomorrow" in the string
                charged_time = f"Tomorrow {charge_time.strftime('%I:%M%p')}"
            
            # Use the icon provided by the charge time calculation
            icon = charge_time_icon

        # Update the "Time Until Charged" sensor
        self.set_state("sensor.time_until_charged", state=time_until_charged, attributes={
            "icon": icon,
            "friendly_name": "Time Until Charged",
            "unique_id": "sensor_time_until_charged"
        })
        
        # Update the "Charged Time" sensor
        self.set_state("sensor.charged_time", state=charged_time, attributes={
            "icon": icon,
            "friendly_name": "Charged Time",
            "unique_id": "sensor_charged_time"
        })

        self.log(f"Updated charge time sensors: Time Until Charged: {time_until_charged}, Charged Time: {charged_time}", level="DEBUG")

    def calculate_charge_time(self, current_soc, average_load):
        try:
            self.log(f"Starting charge time calculation. Current SoC: {current_soc}%, Average Load: {average_load}W", level="DEBUG")
            
            # Get the local timezone
            local_tz = ZoneInfo(self.time_zone)
            # Get the current time in the local timezone
            current_time = self.ensure_timezone_aware(datetime.datetime.now())
            
            # If the battery is already full (>=99%), return immediately
            if current_soc >= self.battery_full_threshold:
                self.log("Battery is already considered full (>=99%)", level="DEBUG")
                return "mdi:battery-charging-100", current_time
            else:
                # Set up the time range for calculation (current time to 2 days from now)
                start_time = current_time.replace(second=0, microsecond=0)
                end_time = start_time + datetime.timedelta(days=2)
                
                # Calculate the battery capacity in Watt-hours
                battery_capacity_wh = self.battery_capacity_ah * self.nominal_voltage
                # Calculate the current energy in the battery
                total_energy_wh = current_soc / 100 * battery_capacity_wh
                charge_time = None

                # Check if we're on shore power
                ac_voltage = self.get_state(self.sensors["ac_volts"])
                is_on_shore_power = ac_voltage not in ['unavailable', 'unknown'] and float(ac_voltage) >= self.shore_power_voltage_threshold

                if is_on_shore_power:
                    # If on shore power, calculate the average load since connecting to shore power
                    shore_power_start_time = self.ensure_timezone_aware(datetime.datetime.fromisoformat(self.get_persistent_data("last_high_voltage_time")))
                    new_average_load = self.calculate_average_load_since(shore_power_start_time)
                    shore_power_charge = abs(new_average_load)
                else:
                    # If not on shore power, use the provided average load
                    new_average_load = average_load
                    shore_power_charge = 0

                # Iterate through each minute from start_time to end_time
                while start_time <= end_time:
                    # Get the current and next hour for solar forecast interpolation
                    current_hour = start_time.replace(minute=0)
                    next_hour = current_hour + datetime.timedelta(hours=1)
                    
                    # Get solar generation forecast for current and next hour
                    current_solar_generation = self.solar_forecast_data.get(current_hour.strftime("%Y-%m-%d %H:%M:%S"), 0)
                    next_solar_generation = self.solar_forecast_data.get(next_hour.strftime("%Y-%m-%d %H:%M:%S"), current_solar_generation)
                    
                    # Interpolate solar generation for the current minute
                    minute_fraction = start_time.minute / 60
                    interpolated_solar_generation = current_solar_generation + (next_solar_generation - current_solar_generation) * minute_fraction
                    
                    # Calculate energy for this minute
                    solar_energy_wh = interpolated_solar_generation / 60  # Convert hourly forecast to per-minute
                    
                    # Calculate energy balance for this minute
                    energy_balance_wh = solar_energy_wh + shore_power_charge / 60 - (0 if is_on_shore_power else new_average_load / 60)

                    # Update total energy in the battery
                    total_energy_wh = min(battery_capacity_wh, max(0, total_energy_wh + energy_balance_wh))
                    # Calculate new SoC
                    soc = (total_energy_wh / battery_capacity_wh) * 100
                    soc = max(0, min(soc, self.battery_full_threshold))

                    self.log(f"Time: {start_time}, Solar generation: {solar_energy_wh}Wh, Energy balance: {energy_balance_wh}Wh, New total energy: {total_energy_wh}Wh, New SoC: {soc}%", level="DEBUG")

                    # If battery is full, set charge time and break the loop
                    if soc >= self.battery_full_threshold:
                        charge_time = start_time
                        self.log(f"Charge time found: {charge_time}, Final SoC: {soc}%", level="DEBUG")
                        break
                    
                    # Move to next minute
                    start_time += datetime.timedelta(minutes=1)

                # Determine the appropriate icon based on whether charge time was found
                if charge_time:
                    charge_time_icon = "mdi:battery-charging"
                    self.log(f"Charge time found: {charge_time}", level="DEBUG")
                else:
                    charge_time_icon = "mdi:battery-alert"
                    self.log("Unable to determine charge time", level="DEBUG")

                return charge_time_icon, charge_time

        except Exception as e:
            self.log(f"Error in calculate_charge_time: {e}")
            return "mdi:battery-unknown", None

    def get_total_energy_production_today(self):
        today = datetime.datetime.now().date()
        total_production = 0
        for timestamp, watt_hours in self.solar_forecast_data.items():
            if parser.isoparse(timestamp).date() == today:
                total_production = watt_hours / 1000  # Convert Wh to kWh
        self.log(f"Total energy production today: {total_production} kWh", level="DEBUG")
        return total_production

    def calculate_peak_soc(self, start_soc, energy_production, average_load):
        # Calculate battery capacity in Watt-hours
        battery_capacity_wh = self.battery_capacity_ah * self.nominal_voltage
        # Calculate starting energy in Watt-hours
        start_wh = start_soc / 100 * battery_capacity_wh
        # Convert energy production from kWh to Wh
        energy_production_wh = energy_production * 1000

        # Calculate net energy gain/loss over 24 hours
        net_energy_wh = energy_production_wh - (average_load * 24)

        # Calculate peak energy, capped at battery capacity
        peak_energy_wh = min(battery_capacity_wh, start_wh + net_energy_wh)

        # Calculate peak SoC as a percentage
        peak_soc = (peak_energy_wh / battery_capacity_wh) * 100

        self.log(f"Peak SoC calculation: start_soc={start_soc}, energy_production={energy_production}, average_load={average_load}, peak_soc={peak_soc}", level="DEBUG")

        # Cap the peak SoC at 99% to avoid showing 100%
        return min(peak_soc, self.battery_full_threshold)

    def get_current_soc(self):
        # Get the current state of charge from the sensor
        soc = float(self.get_state(self.sensors["state_of_charge"]))
        self.log(f"Current SoC: {soc}%", level="DEBUG")
        return soc

    def get_highest_recorded_soc(self, target_date):
        # Calculate the end time as the last second of the target date
        end_time = target_date.replace(hour=23, minute=59, second=59)
        # Calculate the start time as 24 hours before the end time
        start_time = end_time - datetime.timedelta(days=1)
        
        # Construct the URL for the Home Assistant API
        url = f"http://{self.hass_ip}:{self.hass_port}/api/history/period/{start_time.isoformat()}?filter_entity_id={self.sensors['state_of_charge']}&end_time={end_time.isoformat()}"

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

        # Fetch data from the API
        data = self.fetch_data_from_api(url, headers)
        if data is None:
            return 0

        if data and isinstance(data, list) and len(data) > 0:
            # Extract valid SoC values from the data
            soc_values = [float(entry['state']) for entry in data[0] if entry['state'] not in ['unavailable', 'unknown']]
            if soc_values:
                # Return the maximum SoC value
                return max(soc_values)

        return 0

    def calculate_minimum_soc(self, current_soc, average_load):
        
        try:
            # Get the current time in the local timezone
            current_time = self.ensure_timezone_aware(datetime.datetime.now())
            # Set the end time to 24 hours from now
            end_time = current_time + datetime.timedelta(days=1)
            
            self.log(f"Starting minimum SoC calculation. Current time: {current_time}, End time: {end_time}", level="DEBUG")
            self.log(f"Initial SoC: {current_soc}%, Average load: {average_load}W", level="DEBUG")
            
            minimum_soc = current_soc
            time_to_minimum_soc = datetime.timedelta(0)
            # Calculate battery capacity in Watt-hours
            battery_capacity_wh = self.battery_capacity_ah * self.nominal_voltage
            # Calculate current energy in the battery
            total_energy_wh = current_soc / 100 * battery_capacity_wh

            self.log(f"Battery capacity: {battery_capacity_wh}Wh, Initial total energy: {total_energy_wh}Wh", level="DEBUG")

            # Add this logging statement at the beginning of the method
            self.log(f"Solar forecast data at start of calculation: {dict(list(self.solar_forecast_data.items())[:5])}", level="DEBUG")

            prev_solar_generation = 0
            while current_time <= end_time:
                next_hour = current_time + datetime.timedelta(hours=1)
                current_key = current_time.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
                
                # Get solar generation for the current hour
                current_solar_generation = self.solar_forecast_data.get(current_key, prev_solar_generation)
                hourly_solar_generation = max(0, current_solar_generation - prev_solar_generation)
                
                # Calculate energy balance for 1 hour
                energy_balance_wh = hourly_solar_generation - (average_load * 1)  # 1 hour
                total_energy_wh += energy_balance_wh
                
                # Ensure total energy stays within battery capacity limits
                total_energy_wh = max(0, min(total_energy_wh, battery_capacity_wh))
                # Calculate new SoC
                soc = (total_energy_wh / battery_capacity_wh) * 100
                
                self.log(f"Time: {current_time}, Solar generation: {hourly_solar_generation}Wh, Energy balance: {energy_balance_wh}Wh, New total energy: {total_energy_wh}Wh, New SoC: {soc}%", level="DEBUG")
                
                # Update minimum SoC if necessary
                if soc < minimum_soc:
                    minimum_soc = soc
                    time_to_minimum_soc = current_time - self.ensure_timezone_aware(datetime.datetime.now())
                    self.log(f"New minimum SoC found: {minimum_soc}% at {current_time}", level="DEBUG")
                
                current_time = next_hour
                prev_solar_generation = current_solar_generation

            self.log(f"Calculation complete. Final minimum SoC: {minimum_soc}%, Time to minimum: {time_to_minimum_soc}", level="DEBUG")
            return round(minimum_soc, 2), time_to_minimum_soc
        except Exception as e:
            self.log(f"Error in calculate_minimum_soc: {e}")
            return None, None        
    
    def reverse_geocode(self, lat, lon):
        try:
            # Construct URL for OpenStreetMap's Nominatim API
            url = f"https://nominatim.openstreetmap.org/reverse?format=json&lat={lat}&lon={lon}&zoom=18&addressdetails=1"
            # Send GET request to the API
            response = requests.get(url, headers={'User-Agent': self.reverse_geocode_user_agent})
            data = response.json()
            if 'address' in data:
                address = data['address']
                # Try to get the most specific location name available
                name = address.get('amenity') or address.get('building') or address.get('road') or address.get('suburb') or address.get('town') or address.get('city') or "Unknown Location"
                return name
            return "Unknown Location"
        except Exception as e:
            self.log(f"Error in reverse geocoding: {e}", level="ERROR")
            return "Unknown Location"

    def get_location_schema(self, location_name):
        try:
            # Connect to the SQLite database
            conn = sqlite3.connect(self.solar_forecast_db)
            cursor = conn.cursor()
            # Execute SQL query to get the schema for the given location
            cursor.execute('SELECT schema FROM locations WHERE name = ?', (location_name,))
            result = cursor.fetchone()
            conn.close()

            if result and result[0]:
                # If schema exists, parse and return it
                return json.loads(result[0])
            else:
                self.log(f"No schema found for location: {location_name}", level="INFO")
                return None
        except Exception as e:
            self.log(f"Error retrieving location schema for {location_name}: {e}", level="ERROR")
            return None

    def save_location_schema(self, location_name, schema):
        try:
            # Connect to the SQLite database
            conn = sqlite3.connect(self.solar_forecast_db)
            cursor = conn.cursor()
            # Convert schema to JSON string
            schema_json = json.dumps(schema)
            # Try to update existing record
            # Execute SQL query to update the schema for the given location
            cursor.execute('''
                UPDATE locations
                SET schema = ?
                WHERE name = ?
            ''', (schema_json, location_name))
            
            # Check if any rows were affected by the UPDATE query
            if cursor.rowcount == 0:
                # If no rows were updated, it means the location doesn't exist yet
                # So we insert a new record
                cursor.execute('''
                    INSERT INTO locations (name, schema)
                    VALUES (?, ?)
                ''', (location_name, schema_json))
            
            # Commit the changes to the database
            conn.commit()
            
            # Close the database connection
            conn.close()
            
            # Log the successful save operation
            self.log(f"Saved schema for location: {location_name}", level="INFO")
        except Exception as e:
            # Log any errors that occur during the save operation
            self.log(f"Error saving location schema for {location_name}: {e}", level="ERROR")

    def calculate_solar_adjustment_schema(self):
        # Log the start of the calculation process
        self.log("Starting calculation of solar adjustment schema", level="DEBUG")
        
        # Check if solar delta calculation is enabled
        if not self.is_solar_delta_calc_enabled():
            # If disabled, use a default value of 1.0 for all hours
            self.log("Solar delta calculation is disabled. Using default value of 1.0", level="INFO")
            default_schema = {hour: 1.0 for hour in range(24)}
            self.set_solar_production_delta(default_schema)
            return default_schema

        try:
            # Get the arrival time at the current location
            arrival_time = self.get_arrival_time_at_current_location()
            if arrival_time is None:
                # If arrival time is not set, use 30 days ago as the start time
                self.log("Arrival time not set. Using 30 days ago as start time.", level="WARNING")
                arrival_time = datetime.datetime.now(ZoneInfo(self.time_zone)) - datetime.timedelta(days=30)
                self.set_arrival_time_at_current_location(arrival_time)
            else:
                # Convert the arrival time string to a datetime object
                arrival_time = datetime.datetime.fromisoformat(arrival_time)

            # Check if we have a location-specific schema
            if self.current_location_name:
                location_schema = self.get_location_schema(self.current_location_name)
                if location_schema:
                    # If a location-specific schema exists, use it as a base for calculations
                    self.log(f"Using location-specific schema for {self.current_location_name}", level="INFO")
                    return self.calculate_updated_schema(location_schema, arrival_time)

            # If no location-specific schema, calculate from scratch
            return self.calculate_new_schema(arrival_time)

        except Exception as e:
            # Log any errors that occur during the calculation process
            self.log(f"Error in calculate_solar_adjustment_schema: {e}", level="ERROR")
            self.log(f"Error traceback: {traceback.format_exc()}", level="ERROR")
            
            # In case of an error, use a default schema with 1.0 for all hours
            default_schema = {hour: 1.0 for hour in range(24)}
            self.set_solar_production_delta(default_schema)
            return default_schema
            self.log(f"Error in calculate_solar_adjustment_schema: {e}", level="ERROR")
            self.log(f"Error traceback: {traceback.format_exc()}", level="ERROR")
            default_schema = {hour: 1.0 for hour in range(24)}
            self.set_solar_production_delta(default_schema)
            return default_schema

    def calculate_updated_schema(self, base_schema, arrival_time):
        # Check if arrival time is set
        if arrival_time is None:
            return base_schema

        # Calculate the number of days at the current location
        days_at_location = (datetime.datetime.now(ZoneInfo(self.time_zone)) - arrival_time).days

        # If less than a day at the location, return the base schema
        if days_at_location < 1:
            return base_schema

        # Calculate and return an updated schema based on the time spent at the location
        return self.calculate_schema_for_period(arrival_time, base_schema)

    def calculate_schema_for_period(self, start_time, base_schema=None):
        # Set the end time to now
        end_time = datetime.datetime.now(ZoneInfo(self.time_zone))
        self.log(f"Calculating schema from {start_time} to {end_time}", level="DEBUG")
        
        # Limit the calculation period to a maximum of 30 days
        max_days = self.max_calculation_days
        start_time = max(start_time, end_time - datetime.timedelta(days=max_days))

        # Fetch all required data for the date range
        all_forecasts = self.get_solar_forecasts_for_date_range(start_time.date(), end_time.date())
        all_productions = self.get_actual_productions_for_date_range(start_time.date(), end_time.date())
        all_soc_data = self.get_historical_soc_data_range(start_time.date(), end_time.date())

        self.log(f"Collected data for {len(all_forecasts)} days", level="DEBUG")

        # Initialize a dictionary to store adjustment factors for each hour
        adjustment_factors = {hour: [] for hour in range(24)}

        current_date = start_time.date()
        while current_date <= end_time.date():
            # Skip data from 8/23/24 and 8/24/24
            if current_date in [datetime.date(2024, 8, 23), datetime.date(2024, 8, 24)]:
                self.log(f"Skipping excluded date: {current_date}", level="DEBUG")
                current_date += datetime.timedelta(days=1)
                continue

            day_forecast = all_forecasts.get(current_date, {})
            day_production = all_productions.get(current_date, [])

            # Determine the maximum hour to process for this day
            if current_date == end_time.date():
                max_hour = end_time.hour - 1  # Exclude the current hour
            else:
                max_hour = 23

            for hour in range(max_hour + 1):  # +1 here because range is exclusive of the upper bound
                hour_start = self.ensure_timezone_aware(datetime.datetime.combine(current_date, datetime.time(hour, 0)))
                
                # This check is now redundant, but we can keep it for extra safety
                if hour_start >= end_time.replace(minute=0, second=0, microsecond=0):
                    continue

                hour_end = hour_start + datetime.timedelta(hours=1)

                is_valid, max_soc = self.is_soc_valid_for_hour_cached(all_soc_data, hour_start, hour_end)
                forecast_wh = self.get_forecast_wh_for_hour(day_forecast, hour_start, hour_end)
                actual_wh = self.calculate_actual_wh_for_hour(day_production, hour_start, hour_end)

                self.log(f"Date: {current_date}, Hour {hour}: is_valid={is_valid}, max_soc={max_soc}, forecast_wh={forecast_wh}, actual_wh={actual_wh}", level="DEBUG")

                if is_valid and forecast_wh > 0:
                    adjustment_factor = actual_wh / forecast_wh
                    adjustment_factors[hour].append(adjustment_factor)

            current_date += datetime.timedelta(days=1)

        # Calculate the new schema based on the collected adjustment factors
        new_schema = {}
        total_valid_data_points = 0
        for hour in range(24):
            factors = adjustment_factors[hour]
            if len(factors) >= self.min_data_points_for_iqm:  # Use interquartile mean if we have at least 4 data points
                new_schema[hour] = self.interquartile_mean(factors)
                total_valid_data_points += len(factors)
            elif len(factors) > 0:  # Use regular mean if we have 1-3 points
                new_schema[hour] = statistics.mean(factors)
                total_valid_data_points += len(factors)
            elif base_schema and hour in base_schema:  # Use stored value if available
                new_schema[hour] = base_schema[hour]
                self.log(f"No new data for hour {hour}. Using stored value: {base_schema[hour]}", level="DEBUG")
            else:  # Use default value of 1.0 if no data is available
                new_schema[hour] = 1.0
                self.log(f"No data available for hour {hour}. Using default value: 1.0", level="DEBUG")

        # Log summary statistics
        self.log(f"New schema calculated using {total_valid_data_points} valid data points", level="DEBUG")
        self.log(f"Average valid data points per hour: {total_valid_data_points / 24:.2f}", level="DEBUG")
        self.log(f"Data points per hour: {[len(factors) for factors in adjustment_factors.values()]}", level="DEBUG")
        self.log(f"New schema: {new_schema}", level="DEBUG")

        # Save the new schema if we have a current location
        if self.current_location_name:
            self.save_location_schema(self.current_location_name, new_schema)

        # Set the new solar production delta and return the new schema
        self.set_solar_production_delta(new_schema)
        return new_schema

    def get_arrival_time_at_current_location(self):
            # Retrieve the arrival time from persistent storage
            return self.get_persistent_data("arrival_time_at_current_location")

    def set_arrival_time_at_current_location(self, arrival_time):
        # Store the arrival time in persistent storage
        self.set_persistent_data("arrival_time_at_current_location", arrival_time.isoformat())

    def get_solar_forecasts_for_date_range(self, start_date, end_date):
        # Initialize an empty dictionary to store forecasts
        forecasts = {}
        current_date = start_date
        while current_date <= end_date:
            # Format the date as a string
            date_str = current_date.strftime("%Y-%m-%d")
            # Extract forecast data for the current date
            forecasts[current_date] = {k: v for k, v in self.solar_forecast_data.items() if k.startswith(date_str)}
            current_date += datetime.timedelta(days=1)
        return forecasts

    def get_actual_productions_for_date_range(self, start_date, end_date):
        # Initialize an empty dictionary to store production data
        productions = {}
        current_date = start_date
        while current_date <= min(end_date, datetime.datetime.now(datetime.timezone.utc).date()):
            self.log(f"Fetching production data for date range: {current_date}", level="DEBUG")
            # Get production data for the current date
            production_data = self.get_actual_production_for_date(current_date)
            productions[current_date] = production_data
            self.log(f"Production data for {current_date}: {'Available' if production_data else 'Not available'}", level="DEBUG")
            current_date += datetime.timedelta(days=1)
        return productions

    def get_actual_production_for_date(self, date):
        self.log(f"Entering get_actual_production_for_date for {date}", level="DEBUG")
        try:
            # Set the start and end times for the day
            start_time = self.ensure_timezone_aware(datetime.datetime.combine(date, datetime.time.min))
            end_time = self.ensure_timezone_aware(datetime.datetime.combine(date, datetime.time.max))
            
            # Format times in ISO 8601 format
            start_time_str = start_time.isoformat()
            end_time_str = end_time.isoformat()
            
            # URL encode the parameters
            entity_id = urllib.parse.quote(self.sensors["current_solar_production"])
            start_time_encoded = urllib.parse.quote(start_time_str)
            end_time_encoded = urllib.parse.quote(end_time_str)
            
            # Construct the API URL
            url = f"http://{self.hass_ip}:{self.hass_port}/api/history/period/{start_time_encoded}?filter_entity_id={entity_id}&end_time={end_time_encoded}"
            
            # Set up the headers for the API request
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json"
            }
            
            self.log(f"API URL: {url}", level="DEBUG")
            
            # Send the API request
            self.log("Sending API request", level="DEBUG")
            response = requests.get(url, headers=headers, timeout=self.api_request_timeout)
            self.log(f"API Response status code: {response.status_code}", level="DEBUG")
            response.raise_for_status()
            
            self.log(f"API Response content: {response.text[:500]}...", level="DEBUG")  # Log first 500 characters of response
            
            # Parse the JSON response
            data = response.json()
            if data and isinstance(data, list) and len(data) > 0:
                production_data = []
                for entry in data[0]:
                    timestamp = self.ensure_timezone_aware(parser.isoparse(entry['last_changed']))
                    if start_time <= timestamp <= end_time and entry['state'] not in ['unavailable', 'unknown']:
                        try:
                            value = float(entry['state'])
                            production_data.append((timestamp, value))
                        except ValueError:
                            self.log(f"Invalid state value: {entry['state']}", level="WARNING")
                self.log(f"Retrieved {len(production_data)} production data points for {date}", level="DEBUG")
                return production_data
            else:
                self.log(f"No production data available in the response for {date}", level="WARNING")
                return []
        except requests.exceptions.RequestException as e:
            self.log(f"RequestException in get_actual_production_for_date: {e}", level="ERROR")
            self.log(f"RequestException details: {traceback.format_exc()}", level="ERROR")
        except ValueError as e:
            self.log(f"ValueError in get_actual_production_for_date: {e}", level="ERROR")
            self.log(f"ValueError details: {traceback.format_exc()}", level="ERROR")
        except Exception as e:
            self.log(f"Error in get_actual_production_for_date: {e}", level="ERROR")
            self.log(f"Error traceback: {traceback.format_exc()}", level="ERROR")
        finally:
            self.log(f"Exiting get_actual_production_for_date for {date}", level="DEBUG")
        return []

    def get_historical_soc_data_range(self, start_date, end_date):
        # Convert dates to datetime objects with minimum and maximum times
        start_time = self.ensure_timezone_aware(datetime.datetime.combine(start_date, datetime.time.min))
        end_time = min(
            self.ensure_timezone_aware(datetime.datetime.combine(end_date, datetime.time.max)),
            datetime.datetime.now(datetime.timezone.utc)
        )
        # Fetch and return historical SOC data for the specified range
        return self.get_historical_soc_data(start_time, end_time)

    def is_soc_valid_for_hour_cached(self, all_soc_data, hour_start, hour_end):
        # Sort the data by timestamp in descending order
        sorted_data = sorted(all_soc_data, key=lambda x: x[0], reverse=True)
        
        # Find the most recent SOC value before or at the end of the hour
        relevant_data = [entry for entry in sorted_data if entry[0] <= hour_end]
        
        if not relevant_data:
            self.log(f"No SOC data found before or at {hour_end}", level="WARNING")
            return False, None
        
        # Get the most recent SOC value
        max_soc = relevant_data[0][1]  # The first entry is the most recent one
        
        self.log(f"Max SOC for hour {hour_start.hour}: {max_soc}%", level="DEBUG")
        
        # We consider the SOC valid if it's not too high (e.g., 97% or lower)
        is_valid = max_soc <= 97
        
        return is_valid, max_soc

    def get_forecast_wh_for_hour(self, forecast_data, hour_start, hour_end):
        """
        Calculate the forecasted watt-hours for a specific hour.

        Args:
            forecast_data (dict): Dictionary containing forecast data.
            hour_start (datetime): Start time of the hour.
            hour_end (datetime): End time of the hour.

        Returns:
            float: Forecasted watt-hours for the specified hour.
        """
        # Ensure hour_start and hour_end are in the local time zone
        hour_start = hour_start.astimezone(ZoneInfo(self.time_zone))
        hour_end = hour_end.astimezone(ZoneInfo(self.time_zone))
        
        # Format the start and end times as strings to use as keys in the forecast_data dictionary
        start_key = hour_start.strftime("%Y-%m-%d %H:%M:%S")
        end_key = hour_end.strftime("%Y-%m-%d %H:%M:%S")
        
        # Get the watt-hour values for the start and end of the hour
        # If start_key is not in forecast_data, default to 0
        start_wh = forecast_data.get(start_key, 0)
        # If end_key is not in forecast_data, use the start_wh value
        end_wh = forecast_data.get(end_key, start_wh)
        
        # Calculate the forecast watt-hours by subtracting start from end
        forecast_wh = end_wh - start_wh
        self.log(f"Forecast for {hour_start} to {hour_end}: start_wh={start_wh}, end_wh={end_wh}, forecast_wh={forecast_wh}Wh", level="DEBUG")
        return forecast_wh

    def calculate_actual_wh_for_hour(self, production_data, hour_start, hour_end):
        """
        Calculate the actual watt-hours produced for a specific hour.

        Args:
            production_data (list): List of tuples containing timestamp and production value.
            hour_start (datetime): Start time of the hour.
            hour_end (datetime): End time of the hour.

        Returns:
            float: Actual watt-hours produced for the specified hour.
        """
        # Filter production data to only include entries within the specified hour
        relevant_data = [entry for entry in production_data if isinstance(entry, tuple) and len(entry) == 2 and hour_start <= self.ensure_timezone_aware(entry[0]) < hour_end]
        
        if not relevant_data:
            return 0
        
        total_wh = 0
        last_timestamp = None
        last_value = None
        
        for timestamp, value in relevant_data:
            timestamp = self.ensure_timezone_aware(timestamp)
            if isinstance(value, (int, float)):
                if last_timestamp is not None:
                    # Calculate time difference in hours
                    time_diff = (timestamp - last_timestamp).total_seconds() / 3600
                    # Calculate average power between two consecutive readings
                    avg_power = (value + last_value) / 2
                    # Add to total watt-hours
                    total_wh += avg_power * time_diff
                
                last_timestamp = timestamp
                last_value = value
            else:
                self.log(f"Invalid value: {value}", level="WARNING")
        
        self.log(f"Actual production for {hour_start} to {hour_end}: {total_wh}Wh", level="DEBUG")
        return total_wh

    def haversine(self, coord1, coord2):
        """
        Calculate the great circle distance between two points on the earth.

        Args:
            coord1 (tuple): (latitude, longitude) of first point.
            coord2 (tuple): (latitude, longitude) of second point.

        Returns:
            float: Distance between the two points in kilometers.
        """
        R = 6371  # Earth radius in kilometers

        lat1, lon1 = coord1
        lat2, lon2 = coord2

        # Convert latitude and longitude to radians
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)

        # Haversine formula
        a = math.sin(delta_phi/2)**2 + \
            math.cos(phi1) * math.cos(phi2) * \
            math.sin(delta_lambda/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

        return R * c

    def apply_adjustment_schema(self, forecast_data, adjustment_schema):
        """
        Apply an adjustment schema to the forecast data.

        Args:
            forecast_data (dict): Original forecast data.
            adjustment_schema (dict): Schema to adjust the forecast data.

        Returns:
            dict: Adjusted forecast data.
        """
        adjusted_forecast = {}
        
        for timestamp, wh in forecast_data.items():
            dt = datetime.datetime.fromisoformat(timestamp)
            hour = dt.hour
            # Get adjustment factor for the hour, default to 1.0 if not found
            adjustment_factor = adjustment_schema.get(hour, 1.0)
            adjusted_forecast[timestamp] = wh * adjustment_factor
        
        return adjusted_forecast

    def get_soc_at_time(self, soc_data, target_time):
        """
        Get the State of Charge (SoC) at a specific time.

        Args:
            soc_data (list): List of SoC data points.
            target_time (datetime): Time at which to get the SoC.

        Returns:
            float: SoC at the target time, or None if not found.
        """
        valid_entries = [entry for entry in soc_data if self.ensure_timezone_aware(datetime.datetime.fromisoformat(entry['last_changed'])).replace(tzinfo=datetime.timezone.utc) <= target_time]
        if valid_entries:
            return float(valid_entries[-1]['state'])
        return None

    def terminate(self):
        """
        Clean up and terminate the SoCEstimator.
        """
        self.log("SoCEstimator is terminating. Cleaning up callbacks.", level="DEBUG")
        for handle in self.scheduled_callbacks:
            self.cancel_timer(handle)
        self.scheduled_callbacks = []

    def get_historical_soc_data(self, start_time, end_time):
        """
        Retrieve historical State of Charge (SoC) data for a specified time range.

        Args:
            start_time (datetime): Start of the time range.
            end_time (datetime): End of the time range.

        Returns:
            list: List of tuples containing (timestamp, SoC value).
        """
        # Ensure start_time is earlier than end_time
        if start_time >= end_time:
            self.log(f"Invalid time range: start_time {start_time} is not earlier than end_time {end_time}", level="ERROR")
            return []

        # Format times in ISO 8601 format
        start_time_str = start_time.isoformat()
        end_time_str = end_time.isoformat()
        
        # URL encode the parameters
        entity_id = urllib.parse.quote(self.sensors["state_of_charge"])
        start_time_encoded = urllib.parse.quote(start_time_str)
        end_time_encoded = urllib.parse.quote(end_time_str)
        
        # Construct the URL for the Home Assistant API
        url = f"http://{self.hass_ip}:{self.hass_port}/api/history/period/{start_time_encoded}?filter_entity_id={entity_id}&end_time={end_time_encoded}"
        
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            if data and isinstance(data, list) and len(data) > 0:
                # Use a list comprehension for better performance
                soc_data = [(self.ensure_timezone_aware(parser.isoparse(entry['last_changed'])), float(entry['state']))
                            for entry in data[0]
                            if entry['state'] not in ['unavailable', 'unknown']]
                return soc_data
            else:
                self.log(f"No SoC data available for period {start_time} to {end_time}", level="WARNING")
                return []
        except requests.exceptions.RequestException as e:
            self.log(f"Error retrieving historical SoC data: {e}", level="ERROR")
            return []

    def update_load_data(self, kwargs):
        """
        Update the load data with the current load and maintain a 24-hour history.
        """
        try:
            current_time = self.ensure_timezone_aware(datetime.datetime.now())
            current_load = float(self.get_state(self.sensors["dc_loads"]))
            
            # Check if we need to fetch historical data
            if len(self.load_data) < 24 * 60:  # Less than 24 hours of data (assuming 1-minute intervals)
                self.fetch_historical_load_data(current_time)
            
            # Add new data point
            self.load_data.append((current_time, current_load))
            
            # Remove data older than 24 hours
            cutoff_time = current_time - self.data_retention_period
            while self.load_data and self.load_data[0][0] < cutoff_time:
                self.load_data.popleft()
            
            self.last_load_update = current_time.timestamp()
            self.average_load = self.calculate_weighted_average()
            
            # Log the number of data points in the deque
            self.log(f"Load data points in deque: {len(self.load_data)}", level="DEBUG")
            
            self.log(f"Updated load data. Current load: {current_load}W, Average load: {self.average_load}W", level="DEBUG")
            self.calculate_soc()
        except Exception as e:
            self.log(f"Error in update_load_data: {e}", level="ERROR")

    def fetch_historical_load_data(self, end_time):
        """
        Fetch historical load data from Home Assistant API.

        Args:
            end_time (datetime): End time for the historical data fetch.
        """
        try:
            start_time = end_time - self.data_retention_period
            url = f"http://{self.hass_ip}:{self.hass_port}/api/history/period/{start_time.isoformat()}?filter_entity_id={self.sensors['dc_loads']}&end_time={end_time.isoformat()}"
            headers = {"Authorization": f"Bearer {self.access_token}", "Content-Type": "application/json"}
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            if data and isinstance(data, list) and len(data) > 0:
                historical_data = [(self.ensure_timezone_aware(datetime.datetime.fromisoformat(entry['last_changed'])), float(entry['state']))
                                   for entry in data[0]
                                   if entry['state'] not in ['unavailable', 'unknown']]
                
                # Merge historical data with existing data
                self.load_data.extendleft(reversed(historical_data))
                self.log(f"Fetched {len(historical_data)} historical data points", level="DEBUG")
            else:
                self.log("No historical data available", level="WARNING")
        except Exception as e:
            self.log(f"Error fetching historical load data: {e}", level="DEBUG")

    def get_current_soc(self):
        """
        Get the current State of Charge (SoC) from the sensor.

        Returns:
            float: Current State of Charge as a percentage.
        """
        soc = float(self.get_state(self.sensors["state_of_charge"]))
        self.log(f"Current SoC: {soc}%", level="DEBUG")
        return soc

    def set_solar_production_delta(self, schema):
        """
        Set the solar production delta sensor with the current adjustment schema.

        Args:
            schema (dict): The adjustment schema for solar production.
        """
        try:
            current_hour = self.ensure_timezone_aware(datetime.datetime.now()).hour
            
            if schema and current_hour in schema:
                current_adjustment = schema[current_hour]
                state = f"{current_adjustment:.4f}"
            else:
                state = "unknown"

            attributes = {
                "schema": schema,
                "unit_of_measurement": "",
                "friendly_name": "Solar Production Delta",
                "current_hour": current_hour
            }

            set_sensor_state(self, self.sensors["solar_production_delta"], state, attributes)
            self.log(f"Set solar_production_delta sensor: state={state}, current_hour={current_hour}, attributes={attributes}", level="DEBUG")
        except Exception as e:
            self.log(f"Error setting solar_production_delta sensor: {e}")

    @lru_cache(maxsize=128)
    def get_battery_icon(self, soc):
        """
        Get the appropriate battery icon based on the State of Charge (SoC).

        Args:
            soc (float): State of Charge as a percentage.

        Returns:
            str: MDI icon string representing the battery level.
        """
        # Use the pre-computed dictionary for faster lookups
        for threshold, icon in self.battery_icons.items():
            if soc >= threshold:
                return icon
        return "mdi:battery-outline"
