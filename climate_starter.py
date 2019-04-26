#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().run_line_magic('matplotlib', 'inline')
from matplotlib import style
style.use('fivethirtyeight')
import matplotlib.pyplot as plt


# In[2]:


import numpy as np
import pandas as pd


# In[3]:


import datetime as dt


# # Reflect Tables into SQLAlchemy ORM

# In[4]:


# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func


# In[5]:


engine = create_engine("sqlite:///Resources/hawaii.sqlite")


# In[6]:


# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)


# In[7]:


# We can view all of the classes that automap found
Base.classes.keys()


# In[8]:


# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station


# In[9]:


# Create our session (link) from Python to the DB
session = Session(engine)


# # Exploratory Climate Analysis

# In[10]:


# Design a query to retrieve the last 12 months of precipitation data and plot the results
session.query(Measurement.date).order_by(Measurement.date.desc())
latest_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
latest_date = latest_date[0]
latest_date
# Calculate the date 1 year ago from the last data point in the database
year_ago = dt.datetime.strptime(latest_date, "%Y-%m-%d")- dt.timedelta(days=366)
# Perform a query to retrieve the data and precipitation scores
query1=session.query(Measurement.date,Measurement.prcp).filter(Measurement.date>=year_ago).all()
# Save the query results as a Pandas DataFrame and set the index to the date column
precipitation_df = pd.DataFrame(query1, columns = ["date", "precipitation"])
precipitation_df["date"] = pd.to_datetime(precipitation_df["date"], format = "%Y-%m-%d")
precipitation_df.set_index("date", inplace = True)
# Sort the dataframe by date
precipitation_df = precipitation_df.sort_values(by="date", ascending = True)
precipitation_df
# Use Pandas Plotting with Matplotlib to plot the data
precipitation_df.plot(title="Precipitation(2016-2017)")
plt.legend(loc="upper center")
plt.ylim(0,8)
plt.savefig("Images/Precipitation.png", bbox_inches="tight")
plt.show()


# ![precipitation](Images/precipitation.png)

# In[11]:


# Use Pandas to calcualte the summary statistics for the precipitation data
precipitation_df.describe()


# ![describe](Images/describe.png)

# In[12]:


# Design a query to show how many stations are available in this dataset?
session.query(Measurement.station).distinct().count()


# In[13]:


# What are the most active stations? (i.e. what stations have the most rows)?
# List the stations and the counts in descending order.
stations_active = session.query(Measurement.station,func.count(Measurement.station)).group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).all()
stations_active


# In[14]:


# Using the station id from the previous query, calculate the lowest temperature recorded, 
# highest temperature recorded, and average temperature most active station?
most_active_station = stations_active[0][0]
most_active_station

temp_stats = session.query(func.min(Measurement.tobs),func.max(Measurement.tobs),func.avg(Measurement.tobs)).filter(Measurement.station == most_active_station).all()
temp_stats


# In[15]:


# Choose the station with the highest number of temperature observations.
station_most_tempstats = session.query(Measurement.station,func.count(Measurement.tobs)).group_by(Measurement.station).order_by(func.count(Measurement.tobs).desc()).first()
station_most_tempstats = station_most_tempstats [0]
station_most_tempstats
# Query the last 12 months of temperature observation data for this station and plot the results as a histogram
tempstats_12months = session.query(Measurement.tobs).filter(Measurement.date>=year_ago).filter(Measurement.station == station_most_tempstats).all()

temperature_observation = pd.DataFrame(tempstats_12months, columns=["temperature"])

temperature_observation.plot.hist(bins=12, title = "Frequency vs Temp Histogram")
plt.savefig("Images/frequency.png", bbox_inches="tight")
plt.show()


# ![precipitation](Images/station-histogram.png)

# In[16]:


# This function called `calc_temps` will accept start date and end date in the format '%Y-%m-%d' 
# and return the minimum, average, and maximum temperatures for that range of dates
def calc_temps(start_date, end_date):
    """TMIN, TAVG, and TMAX for a list of dates.
    
    Args:
        start_date (string): A date string in the format %Y-%m-%d
        end_date (string): A date string in the format %Y-%m-%d
        
    Returns:
        TMIN, TAVE, and TMAX
    """
    
    return session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).        filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()

# function usage example
print(calc_temps('2012-02-28', '2012-03-05'))


# In[17]:


# Use your previous function `calc_temps` to calculate the tmin, tavg, and tmax 
# for your trip using the previous year's data for those same dates.
trip_temps= calc_temps('2012-02-28', '2012-03-05')

d1= dt.date(2012,2,28)
d2= dt.date(2012,3,5)

trip_days = d2-d1


# In[18]:


# Plot the results from your previous query as a bar chart. 
# Use "Trip Avg Temp" as your Title
# Use the average temperature for the y value
# Use the peak-to-peak (tmax-tmin) value as the y error bar (yerr)
trip_temps[0][1]

trip_dict = {'tmin':trip_temps[0][0],'tavg':trip_temps[0][1],'tmax':trip_temps[0][2]}
yerr=trip_dict['tmax'] - trip_dict['tmin']

plt.figure(figsize=(2,6))
plt.bar(1,trip_dict['tavg'],yerr=yerr,align='center',color='green', alpha= .75)
plt.xticks([])
plt.ylim(0,100)
plt.ylabel("Temp(F)")
plt.xlabel("Trip Duration")
plt.grid(color='g', linestyle='-', linewidth=2,)
plt.suptitle("Average Temperature of Trip", size = 15)
plt.show()


# In[19]:


# Calculate the total amount of rainfall per weather station for your trip dates using the previous year's matching dates.
# Sort this in descending order by precipitation amount and list the station, name, latitude, longitude, and elevation

# Check relationships
session.query(Measurement.station,Station.station).filter(Measurement.station==Station.station).all()
session.query(Measurement.id,Station.id).filter(Measurement.station==Station.station).all()

# Check number of total records match
session.query(Measurement.station).count()
session.query(Measurement.station,Station.station).filter(Measurement.station==Station.station).count()

# Finally do Join and Calculate rainfall per weather station for our trip!
trip_rainfall_per_station=session.query(Station.station, Station.name, 
            Station.latitude, Station.longitude, 
              Station.elevation, Measurement.prcp).\
filter(Measurement.station==Station.station).\
filter(Measurement.date >='2012-02-28').\
filter(Measurement.date <= '2012-03-05').\
order_by(Measurement.prcp.desc()).all()

trip_rainfall_per_station


# ## Optional Challenge Assignment

# In[20]:


# Create a query that will calculate the daily normals 
# (i.e. the averages for tmin, tmax, and tavg for all historic data matching a specific month and day)
def daily_normals(date):
    """Daily Normals.
    
    Args:
        date (str): A date string in the format '%m-%d'
        
    Returns:
        A list of tuples containing the daily normals, tmin, tavg, and tmax
    
    """
    
    sel = [func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)]
    return session.query(*sel).filter(func.strftime("%m-%d", Measurement.date) == date).all()
    
daily_normals("01-01")

trip_dates = pd.date_range('2012-02-28', '2012-03-05', freq='D')
print(trip_dates)
month_day = trip_dates.strftime("%m-%d")
print(month_day)
normals = []
for date in month_day:
    normals.append(*daily_normals(date))
    
normals


# In[21]:


# calculate the daily normals for your trip
# push each tuple of calculations into a list called `normals`

# Set the start and end date of the trip

# Use the start and end date to create a range of dates

# Stip off the year and save a list of %m-%d strings

# Loop through the list of %m-%d strings and calculate the normals for each date
daily_normals_df = pd.DataFrame()
daily_normals_df = pd.DataFrame(normals, 
                                     index=trip_dates, columns=[ 'tmin', 'tavg', 'tmax'])
daily_normals_df.index.name = 'date'
daily_normals_df


# In[22]:


# Load the previous query results into a Pandas DataFrame and add the `trip_dates` range as the `date` index

daily_normals_df.plot(kind='area',x_compat=True, alpha=.2, stacked=False)
plt.xticks(rotation=45)
# ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d/%Y'))
plt.tight_layout()


# In[23]:


# Plot the daily normals as an area plot with `stacked=False`


# In[ ]:





# In[ ]:




