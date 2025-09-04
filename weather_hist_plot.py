import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# -------------- CONFIGURATION --------------
LOCATION = {
    "latitude": 32.722222,
    "longitude": -110.644167,
    "timezone": "America/Los_Angeles"
}
DATE_RANGE = {
    "start": "2024-01-01",
    "end": "2024-12-31"
}
# ← Choose from: "temperature_2m", "precipitation", "rain", "snowfall", etc.
VARIABLE = "temperature_2m"
AGG_METHOD = "mean"          # "mean", "max", "min", or "sum"
# -------------------------------------------

# Units mapping (can be expanded as needed)
UNITS = {
    "temperature_2m": "°F",
    "relative_humidity_2m": "%",
    "precipitation": "mm",
    "rain": "mm",
    "snowfall": "mm",
    "snow_depth": "cm",
    "wind_speed_10m": "m/s",
    "wind_direction_10m": "°"
}

# Optional Fahrenheit conversion for temperature
CONVERT_TO_FAHRENHEIT = VARIABLE == "temperature_2m"

# Setup Open-Meteo API
cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# API Request
url = "https://archive-api.open-meteo.com/v1/archive"
params = {
    "latitude": LOCATION["latitude"],
    "longitude": LOCATION["longitude"],
    "start_date": DATE_RANGE["start"],
    "end_date": DATE_RANGE["end"],
    "hourly": [VARIABLE],
    "timezone": LOCATION["timezone"]
}
response = openmeteo.weather_api(url, params=params)[0]

# Extract data
hourly = response.Hourly()
values = hourly.Variables(0).ValuesAsNumpy()
timestamps = pd.date_range(
    start=pd.to_datetime(hourly.Time(), unit="s",
                         utc=True).tz_convert(LOCATION["timezone"]),
    end=pd.to_datetime(hourly.TimeEnd(), unit="s",
                       utc=True).tz_convert(LOCATION["timezone"]),
    freq=pd.Timedelta(seconds=hourly.Interval()),
    inclusive="left"
)

# Build DataFrame
df = pd.DataFrame({VARIABLE: values}, index=timestamps)

# Optional: convert temperature to Fahrenheit
if CONVERT_TO_FAHRENHEIT:
    df[VARIABLE] = (df[VARIABLE] * 9/5) + 32

if VARIABLE in {"precipitation", "rain", "snowfall"}:
    df[VARIABLE] = df[VARIABLE] / 25.4  # mm → inches
    UNITS[VARIABLE] = "in"

# Daily and monthly aggregation
daily = df.resample("D").agg(AGG_METHOD)
monthly = daily.resample("M").agg(AGG_METHOD)

# Get unit label
unit = UNITS.get(VARIABLE, "")  # default to empty if unknown

# Plotting
fig, ax = plt.subplots(figsize=(10, 5))
months = monthly.index

ax.plot(months, monthly[VARIABLE], marker="o",
        label=VARIABLE.replace("_", " ").title())
ax.set_title(f"Monthly {AGG_METHOD.title()} of {
             VARIABLE.replace('_', ' ').title()} - 2024")
ax.set_xlabel("Month")
ax.set_ylabel(f"{VARIABLE.replace('_', ' ').title()} ({unit})")
ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
ax.xaxis.set_major_locator(mdates.MonthLocator())
ax.grid(True, linestyle="--", alpha=0.5)
ax.legend()
plt.tight_layout()
plt.show()
