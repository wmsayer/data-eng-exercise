# data-eng-exercise

This repository for [Big Dorks Only](https://bigdorksonly.io/) - a side project I started in January 2023 to gain more experience with modern data stack tools. Big Dorks Only is a basic webapp built using Python, Flask, and Dash, served on an AWS EC2 instance built to provide crypto-related data with basic transformations using automated ETL processes via dbt Cloud.

#### Data Life Cycle:
- Data is pulled from various public cryptocurrency APIs and stored into my Snowflake database
- dbt is used to transform the data within Snowflake and load it into a new “output” schema
- The webapp serves data directly from Snowflake
- Data logging and ETL processes are fully automated - currently scheduled to run every 5 minutes

## Built With
- [dbt Cloud](https://www.getdbt.com/) - ETL processes
- [Snowflake](https://www.snowflake.com/en/) - Cloud-based SQL database
- [AWS](https://aws.amazon.com/) - EC2 Ubuntu instance to host the webapp 
- [Flask](https://flask.palletsprojects.com/) - Python webapp framework
- [Dash](https://plotly.com/dash/) - Data dashboards and webapp components
- [Nginx](https://www.nginx.com/) - Front-end reverse proxy for the uWSGI server

## Directories
````
.
├── application       # webapp server files
├── data              # local cache directory for data logging (Git ignores all CSV's in this dir)
├── models            # dbt models (tables/views) that run during dbt job
├── seeds             # dbt tables loaded from CSVs during dbt job used to "seed" dbt models
├── snowflake         # worksheet of Snowflake commands to build out schema for raw data
├── tests             # extra tests run on Snowflake db during dbt job
├── dbt_project.yml   # dbt project configuration
├── requirements.txt  # Python env for local development (not all needed for prod server)
└── README.md
````
