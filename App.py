#!/usr/bin/env python
# coding: utf-8

# In[ ]:



import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from flask import Flask, jsonify
import datetime as dt

#setting up database
#creating the engine

engine = create_engine("sqlite:///Resources/hawaii.sqlite")

#reflecting an existing db in a new modal
Base = automap_base()
#reflecting the tables
Base.prepare(engine, reflect=True)

#saving reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station


#creating a session link from Python
session = Session(engine)

#setting up Flask
app = Flask(__name__)

#Flask Routes
@app.route("/")
def welcome():
    """Listing all available api routes"""
    return(
        f"Availabile Routes: <br>"
        f"/api/v1.0/precipitation <br>"
        f"/api/v1.0/stations <br>"
        f"/api/v1.0/tobs"
    )

@app.route("/api/v1.0/precipitation")
def prcpp():
    #querying the percipitation
    results = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    results = results[0]
    year_ago = dt.datetime.strptime(results, "%Y-%m-%d")- dt.timedelta(days=366)
    results1=session.query(Measurement.date,Measurement.prcp).filter(Measurement.date>=year_ago).all()
    
    #returnig the dict in a jsonified formart
    prcp_results= dict(results1)
    return jsonify(prcp_results)

@app.route("/api/v1.0/stations")
def stat():
    #querying the stations
    st_query = session.query(Measurement.station).group_by(Measurement.station).all()
    stat_results = list(st_query)
    #returnig the list in a jsonified formart
    return jsonify(stat_results)

@app.route("/api/v1.0/tobs")
def tobs():
    #query to get dates and temperature for a year
    max_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    max_date = max_date[0]
    year = dt.datetime.strptime(max_date, "%Y-%m-%d") - dt.timedelta(days=366)
    temp_results = session.query(Measurement.date, Measurement.tobs).filter(Measurement.date >= year).all()
    #returnig the list in a jsonified formart

    temp_list = list(np.ravel(temp_results))
    return jsonify(temp_list)

if __name__ == '__main__':
    app.run(debug=True)

