# State of Charge Estimator

State of Charge Estimator is a robust AppDaemon script written in Python which calculates and reports the estimated upcoming state of charge for a solar-charged DC system. It reports what the peak state of charge is expected to be for the next two days, what time the batteries should fully recharge next, and what the lowest point the batteries will reach if all else is equal.

To do this it employs some fairly complex logic. It uses GPS location data to become aware of its own position and keep a database of known locations. It calculates a delta between the forecasted solar data and what was actually produced over recent days. This calculation resets when moved, and remembers what it was before when you re-visit the same location. This helps not only to calculate and factor in things like panel degredation and dust; but also to adjust for shading or obscured horizons. To anticipate drops in solar production caused by resistance from charged batteries, this logic also knows to ignore data when the state of charge is high.

This system has been tested only with flat-mounted panels charging through a solar charge controller. It currently does not have logic to deal with angled panels, but this could be added fairly easily. 

This uses the forecast.solar API, which is a free API and the same one the Home Assistant Energy Dashboard uses. However it is rate limited, so you may want to adjust those intervals in the script if it becomes an issue. It does store data so that historic data can be used for the delta calculations, and it does not call the API outside of the interval that's set even if the script reboots.

Read these instructions carefully and pay special attention to the required sensors. This does require a fairly well-connected solar charging and battery system in order to perform these complex calculations. It simply won't work without them.

# Installation Instructions:

1. Install [AppDaemon](https://github.com/hassio-addons/addon-appdaemon), available in the Home Assistant Add-On Store
2. Copy soc_estimator.py to your AppDaemon `apps` folder.
3. Add the following to the AppDaemon `apps.yaml`:
4.  Set up a [long-lived access token](https://developers.home-assistant.io/docs/auth_api/#long-lived-access-token) in Home Assistant
```
soc_estimator:
module: soc_estimator
class: SoCEstimator
```
5. In `soc_estimator.py`, make sure to configure the sensors and parameters in `initialize`:
```
# Set up battery and solar system parameters
self.battery_capacity_ah  =  200  # Battery capacity in Amp hours.
self.nominal_voltage  =  12.8  # Nominal battery bank voltage.
self.solar_capacity_kw  =  0.4  # Solar capacity in kilowatts.
self.hass_ip  =  "homeassistant.local"  # Home Assistant IP address / hostname.
self.hass_port  =  8123  # Home Assistant port.
self.access_token  =  "LONG-LIVED-ACCESS-TOKEN"  # Home Assistant long-lived access token.
self.api_data_file  =  "/config/apps/storage/solar_forecast_data.json"  # File for storing solar forecast data.
self.soc_adjustment_threshold  =  self.args.get("soc_adjustment_threshold", 97) # The threshold at which the script will consider the SOC valid for solar delta schema calculations. At a high SOC, the batteries will accept less current which will throw off the solar delta calculations.
# Define sensors used by the estimator
self.sensors  = {
# Input sensors (set and provide these for the script to work)
"state_of_charge": "sensor.battery_percent", # State of charge sensor. Expects a percent value (0-100).
"gps_latitude": "sensor.gps_latitude", # GPS latitude sensor. Expects a float value.
"gps_longitude": "sensor.gps_longitude", # GPS longitude sensor. Expects a float value.
"dc_loads": "sensor.dc_loads", # DC loads sensor. Expects a float value. Real-time value in watts. Loads only (not solar production).
"ac_volts": "sensor.watchdog_voltage_line_1", # AC Voltage sensor. Expects a float value. Real-time value in volts. Used to detect if the vehicle is on shore power. Optional, can be omitted.
# Output sensors (the script will create these; but you can override the names if you want to)
"average_load": "sensor.average_load", # Average load sensor. Creates a new sensor with the average load over the last 24 hours as calculated by the script. Not needed; but gives you the ability to add it to your dashboard.
"calculated_energy_production_today_remaining": "sensor.calculated_energy_production_today_remaining", # Calculated energy production today remaining sensor. Expects a float value. Real-time value in watts.
"calculated_energy_production_tomorrow": "sensor.calculated_energy_production_tomorrow", # Calculated energy production tomorrow sensor. Real-time value in watts.
"time_until_charged": "sensor.time_until_charged", # Time until charged sensor. Outputs similar to other Home Assistant time sensors. i.e., "In 3 hours"
"current_solar_production": "sensor.current_solar_production", # Current solar production sensor. Real-time value in watts.
"solar_production_delta": "sensor.solar_production_delta", # Solar production delta sensor. Tells you how much the forecast is being adjusted to account for historic actual data.
"expected_peak_soc_today": "sensor.expected_peak_soc_today", # Expected peak SOC today sensor. Outputs a percentage value (0-100). Shows the expected peak SOC for today based on the solar forecast.
"expected_peak_soc_tomorrow": "sensor.expected_peak_soc_tomorrow", # Expected peak SOC tomorrow sensor. Outputs a percentage value (0-100). Shows the expected peak SOC for tomorrow based on the solar forecast.
"expected_minimum_soc": "sensor.expected_minimum_soc", # Expected minimum SOC sensor. Outputs a percentage value (0-100). Shows the expected minimum SOC for the next 24 hours based on the solar forecast.
"time_to_minimum_soc": "sensor.time_to_minimum_soc", # Time to minimum SOC sensor. Outputs similar to other Home Assistant time sensors. i.e., "In 3 hours"
"charged_time": "sensor.charged_time"  # Charged time sensor. Outputs the exact time the batteries are expected to be fully charged. i.e., "Tomorow 11:34AM"
}
self.update_interval  =  30*60  # How often to update the sensors in seconds.
self.api_update_interval  =  60*60  # How often to pull the latest solar forecast data from the API in seconds. NOTE: This API is free, but has a rate limit. It's the same API used by the Home Assistant Energy Dashboard; so if you're using that, consider the impact on the rate limit.
```
**Explanation of Sensors and Parameters:**
`self.battery_capacity_ah`: The capacity of your battery bank in amp hours

`self.nominal_voltage`: The nominal voltage of your battery bank. For example, a 12v lead-acid bank would be `12`, and 12v LiFePO4 bank would be `12.8`, and 24v LiFePO4 bank would be `25.6`, etc. The script uses this value for some of its calculations, so it must be set correctly.

`self.solar_capacity_kw`: The total installed capacity of your solar panels, in kilowatts. In the default example of a 400w install, the value here would be `0.4`. This is passed along to the forecast API to produce forecasted values. Over time, the script will adjust its calculations based on your actual production.

`self.hass_ip`: IP / hostname for your Home Assistant install. 

`self.hass_port`: Port for your Home Assistant install (default is 8123).

`self.access_token`: The access token you generated above. This is used because we access historic data from the Home Assistant database using the Home Assistant API. 

`self.api_data_file`: The script will create a persistent data file to keep track of things like API calls even if the script crashes or reboots. The default is `/config/apps/storage/solar_forecast_data.json`. Note that when using AppDaemon, `/config/` points to AppDaemon's config folder, not Home Assistant's. 

`self.soc_adjustment_threshold`: A value, above which, the script will not consider data useful for delta calculations. As batteries charge, internal resistance increases. A good value for lithium batteries is 97 or 98. For lead acid, it may need to be lower. Otherwise the script will incorrectly assume your solar panels are under-performing when in fact it's just that the batteries are nearly full and aren't accepting a full charge.

In the `self.sensors` section, a number of sensors are created. This generally should not be adjusted but I left it there in case you want to rename something. However, the `# Input sensors` absolutely must be set.

`state_of_charge`: This is a real-time percentage value showing your batteries current state of charge.

`gps_latitude`: GPS Latitude from a GPS sensor to determine location. Location is used to keep a database of known locations and what sort of solar performance was experienced there. It's also used to know how far back to read actual performance to calculate. If you've recently moved, it won't look at any data from before you arrived at your current location.

`gps_longitude`: Same as above.

`dc_loads`: Be careful with this one! Some battery monitoring shunts and charge controllers will report an *overall* value which will not work here. We need loads only, a positive value in watts. The script calculates a weighted average load using an interquartile method to help determine when things will be charged or how low batteries will get. Without this value, the script cannot produce accurate results. If your system doesn't provide this, you could try creating a helper or a template sensor that substracts your solar production in watts from an "overall load" produced by a battery monitoring shunt, to get a "net load" value. Again, we want JUST what the loads on the DC system are, not what the loads are plus whatever incoming current is. That'll throw all the calculations off.

`ac_volts`: This sensor is optional. However, it is *strongly* recommended if you intend to use this in a system that will occasionally use shore power that charges your batteries. The introduction of a shore-power based charger (including a generator) will throw off all your calculations and generate bad data. If `ac_volts` reports a value above the threshold based on whatever sensor you set, the script will flag the data logged during this period and not use it for calculations. As a bonus; it will still take solar forecast data *and* load data if your DC load sensor goes into negative when charging off of shore power, and will still give you a valid time to charge and charged time!

`current_solar_production`: A value in watts that represents the output of your solar charge controller. This is used by the script to learn, over time, what your actual performance is, and to adjust the incoming forecast data accordingly.

# Setting up the Sensors	

Once the script is properly configured; you're left with data! Here's what you'll get.

`sensor.average_load`: This is the scripts calculated average load for your system. It uses a weighted average weighting recent data; and an interquartile method to essentially filter out short meaningless spikes. (In the scheme of 24 hours, running a blender for 15 seconds isn't meaningful data but could dramatically skew an average.) This works pretty good in systems with fairly steady loads; and smooths out things like refrigerators or air conditioners cycling on and off.

`sensor.calculated_energy_production_today_remaining`: How much more solar production in watt-hours you can expect today.

`sensor.calculated_energy_production_tomorrow`: The currently forecasted solar production for tomorrow

`sensor.time_until_charged`: This outputs a timestamp that Home Assistant will interpret the way other sensors work. So it'll display on your dashboard as something like "In 3 hours" or "In 22 minutes"

`sensor.charged_time`: Displays the actual time that your battery bank will become fully charged. Presented as a time in your local time zone, 12 hour time, with "Tomorrow" appended if it expects a full charge tomorrow instead of today. **Note:** The script assumes 99% as "full". This is intentional and not a bug. Small variations in battery monitors and similar systems can sometimes cause a fully charged battery to report something like 99.2% or whatever. This is configurable in the script in `self.battery_full_threshold`. This prevents a situations where calculations are thrown off because a battery is fully charged but the script thinks it isn't.

`sensor.solar_production_delta`: This is mostly a debugging tool. It will show you the current delta value for the current hour. In attributes field, it'll show you the delta for the entire day. This is the value it is multiplying the forecast by to give a real-world charge time estimate instead of a "in perfect conditions" estimate.

`sensor.expected_peak_soc_today`: This is the highest state of charge the script expects before midnight. Most days, hopefully, this reads "100%". But if your load is unusually high or production is poor this will be a first indicator that you're not going to reach a full charge today.

`sensor.expected_peak_soc_tomorrow`: Same concept as above, but it gives you the value for 'tomorrow' (after midnight tonight, and before midnight tomorrow night). Helpful for planning. Bad weather predicted tomorrow will cause the solar forecast to predict a low production. And that low production could present itself as effectively a warning that you won't have enough solar to recharge tomorrow, so you can plan accordingly.

`sensor.expected_minimum_soc`: This is the lowest state of charge the script expects. The logic here is that it iterates hour by hour forward up to 48 hours. There is a point at which solar production should exceed loads. It basically logs what the state of charge will be at that point. In a typical day, this will be early morning. But if weather is strange and the batteries will actually hit their lowest later in the day, this will output accordingly. You can use this sensor as a guide to basically tell you the lowest you can expect the batteries to get over the next day. 

`sensor.time_to_minimum_soc`: This will output a value in hours for when the above mentioned minimum soc is expected. 

Simply add whichever of those sensors you find useful to your dashboard(s). The script will take care of creating them in Home Assistant if they don't exist. 

