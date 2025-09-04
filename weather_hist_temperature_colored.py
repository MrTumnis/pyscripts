import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

# Setup Open-Meteo client
cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

location = 'Mammoth'

# Weather API request
url = "https://archive-api.open-meteo.com/v1/archive"
params = {
    "latitude": 32.722222,
    "longitude": -110.644167,
    "start_date": "2024-01-01",
    "end_date": "2024-12-31",
    "hourly": ["temperature_2m"],
    "timezone": "America/Los_Angeles"
}
response = openmeteo.weather_api(url, params=params)[0]

# Extract hourly data
hourly = response.Hourly()
temps = hourly.Variables(0).ValuesAsNumpy()

dates = pd.date_range(
    start=pd.to_datetime(hourly.Time(), unit="s",
                         utc=True).tz_convert("America/Los_Angeles"),
    end=pd.to_datetime(hourly.TimeEnd(), unit="s",
                       utc=True).tz_convert("America/Los_Angeles"),
    freq=pd.Timedelta(seconds=hourly.Interval()),
    inclusive="left"
)

# Create DataFrame
df = pd.DataFrame({"temperature_2m": temps}, index=dates)

# Resample to daily highs and lows
daily_highs = df.resample("D").max()
daily_lows = df.resample("D").min()

daily_highs["temperature_2m"] = (daily_highs["temperature_2m"] * 9/5) + 32
daily_lows["temperature_2m"] = (daily_lows["temperature_2m"] * 9/5) + 32

monthly_highs = daily_highs.resample("M").mean()
monthly_lows = daily_lows.resample("M").mean()

# Confidence interval using daily std dev (also convert to Fahrenheit)
high_std = daily_highs.resample("M").std()
low_std = daily_lows.resample("M").std()

# Plot
fig, ax = plt.subplots(figsize=(10, 5))

months = monthly_highs.index
ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
ax.xaxis.set_major_locator(mdates.MonthLocator())

# Plot high temps
ax.plot(months, monthly_highs, color='darkred', label='Avg High')
ax.fill_between(months,
                monthly_highs["temperature_2m"] - high_std["temperature_2m"],
                monthly_highs["temperature_2m"] + high_std["temperature_2m"],
                color='red', alpha=0.2)

# Plot low temps
ax.plot(months, monthly_lows, color='navy', label='Avg Low')
ax.fill_between(months,
                monthly_lows["temperature_2m"] - low_std["temperature_2m"],
                monthly_lows["temperature_2m"] + low_std["temperature_2m"],
                color='blue', alpha=0.2)

# Annotate extremes
max_day = monthly_highs["temperature_2m"].idxmax()
max_temp = monthly_highs["temperature_2m"].max()
ax.text(max_day, max_temp + 2,
        f"{max_day.strftime('%b %d')}\n{int(max_temp)}°F", ha="center", fontsize=16)

min_day = monthly_lows["temperature_2m"].idxmin()
min_temp = monthly_lows["temperature_2m"].min()
ax.text(min_day, min_temp - 5,
        f"{min_day.strftime('%b %d')}\n{int(min_temp)}°F", ha="center", fontsize=16)

# Seasonal shading
hot = (months.month >= 6) & (months.month <= 9)
cool1 = (months.month <= 2)
cool2 = (months.month == 12)

ax.axvspan(months[cool1][0], months[cool1][-1], color='blue', alpha=0.1)
ax.axvspan(months[hot][0], months[hot][-1], color='red', alpha=0.1)
ax.axvspan(months[cool2][0], months[cool2][-1], color='blue', alpha=0.1)

# Labels and limits
ax.set_title(
    f"{location} Monthly High and Low Temperatures - 2024", fontsize=20)
ax.set_xlabel("Month", fontsize=20)  # ← Add this line
ax.set_ylabel("Temperature (°F)", fontsize=20)
ax.set_ylim(0, 110)
ax.set_xlim(months[0], months[-1])

# Tick label size
ax.tick_params(axis='x', labelsize=14)
ax.tick_params(axis='y', labelsize=14)

ax.grid(True, linestyle='--', alpha=0.5)
ax.legend(fontsize=14)

plt.tight_layout()
plt.show()
