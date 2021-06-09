# Weather Data for June 3rd - June 10th

This document describes the approach I took to writing ETL jobs using Python, Pandas, GCP storage and Biqquery for Weather data.
I used openweathermap's One Call API.  I made API calls to get one week of data including previous days.  
The topis covered in this topic are following.
    
    - Data
    - Set up used for the ETL Project
    - Structure of the Repository
    - Project Dependencies
    
## Data
    https://openweathermap.org/api
## Setup:
    - Installed python 3.7.4
    - Pandas 1.2.4
    - gsutil 4.62
## Managing Project Dependencies
    I used 'venv' for managing project dependencies and Python environments(virtual environment).
    Venv comes with Python.
## Structure of the Repository
    |- readme.md
    |- src
        |etl.py
    |curated
        |output_file
## Testing
ETL testing will be added later part of the project
    



