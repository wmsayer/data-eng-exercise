# data-eng-exercise

This repository for [Big Dorks Only](https://bigdorksonly.io/) - a side project I started in January 2023 to gain more experience with modern data stack tools. Big Dorks Only is a basic webapp built using Python, Flask, and Dash, served on an AWS EC2 instance built to provide crypto-related data with basic transformations using automated ETL processes via dbt Cloud.

#### Data Life Cycle:
- Data is pulled from various public cryptocurrency APIs and stored into my Snowflake database
- dbt is used to transform the data within Snowflake and load it into a new “output” schema
- The webapp serves data directly from Snowflake
- Data logging and ETL processes are fully automated - currently scheduled to run every 6 hours

### Built With
- [dbt Cloud](https://www.getdbt.com/) - ETL processes
- [Snowflake](https://www.snowflake.com/en/) - Cloud-based SQL database
- [AWS](https://aws.amazon.com/) - EC2 Ubuntu instance to host the webapp 
- [Flask](https://flask.palletsprojects.com/) - Python webapp framework
- [Dash](https://plotly.com/dash/) - Data dashboards and webapp components
- [Nginx](https://www.nginx.com/) - Front-end reverse proxy for the uWSGI server

## Table of Contents

- [Directories](#directories)
- [Setup Snowflake Account](#setup-snowflake-account)
- [Setup dbt Account](#setup-dbt-account)
- [Setup Data-Logging Instance](#setup-data-logging-instance)
- [Setup AWS Webapp Server](#setup-aws-webapp-server)

 
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

return to [TOC](#table-of-contents)

## Setup Snowflake Account

1. Setup a free-tier [Snowflake](https://www.snowflake.com/en/) account.
2. Save the following credentials somewhere safe:

```commandline
SNOWFLAKE_USER=XXXXXX
SNOWFLAKE_PWD=XXXXXXXXXXXXXXXXXX
SNOWFLAKE_ACCOUNT=XXXXXX.XX-XXXX-X.aws
```

3. Setup yor Snowflake environment with the commands in `snowflake/snowflake_worksheet.txt`
4. Enable `Anaconda` via Admin > Billing & Terms. This allows dbt Python models to use Anaconda packages like `pandas`.

return to [TOC](#table-of-contents)

## Setup dbt Account

### Create dbt Project

1. Setup a free-tier [dbt](https://www.getdbt.com/) account.
2. Connect to the Snowflake instance w/ output schema as `dbt_output_dev`.
3. Connect dbt to this repo with the Git Clone option

```commandline
git@github.com:wmsayer/data-eng-exercise.git
```

4. Add the SSH key to this repo's [Deploy Keys](https://docs.getdbt.com/docs/collaborate/git/import-a-project-by-git-url#section-managing-deploy-keys)
5. Create the project
6. Go to the IDE in the deploy tab and move to the `feature/dev-dbt` branch

### Create Production dbt Job

1. Go to Deploy > Environments and create a new environment named `Production`
2. Connect to Snowflake w/ output schema as `dbt_output_prod`.
3. Go to Deploy > Jobs and create a new job with the Production environment.
4. Set the schedule on the job to `30 */6 * * *` so it runs every 6 hours, 30 minutes past the hour (assuming the data logger is scheduled to run every 6 hours).

return to [TOC](#table-of-contents)

## Setup Data-Logging Instance

### Launch & Connect to the instance

Setup an Ubuntu instance on AWS with an SSH security rule that only allows your IP to connect. Save the `.pem` key locally so you can connect via SSH.

Connect to your instance via SSH:

```
ssh -i XXXXXX.pem ubuntu@ec2-XXXXX.XXXXX.compute.amazonaws.com
```

Update the machine’s local package index:

```
sudo apt update
```

### Install Python & venv

```commandline
sudo apt install python3-pip python3-dev build-essential libssl-dev libffi-dev python3-setuptools
sudo apt install python3-venv
```

Clone the repo:

`git clone https://github.com/wmsayer/data-eng-exercise.git`

Setup the venv:

```commandline
cd data-eng-exercise/
python3 -m venv venv
```

Launch the venv:

```
source venv/bin/activate
```

Install Python packages in venv:

```
pip install -r application/server/logger_requirements.txt
```

### Declare local environment variables

Open your `.bashrc` profile:

```commandline
nano ~/.bashrc
```

Declare all required variables:

```commandline
export SNOWFLAKE_USER=XXXXXXX
export SNOWFLAKE_PWD=XXXXXXX
export SNOWFLAKE_ACCOUNT=XXXXXXX.XXXXXX.aws
export SNOWFLAKE_DB=XXXXXXX
export FLIPSIDE_API_KEY=XXXXXXX
export CRYPTOWATCH_API=XXXXXXX
```

### Install CRON

Following this [tutorial](https://www.digitalocean.com/community/tutorials/how-to-use-cron-to-automate-tasks-ubuntu-1804)

```commandline
sudo apt install cron
```

Enable CRON to run in the background:

```
sudo systemctl enable cron
```

### Setup the CRON job

Open the crontab file (your first time running this, select `nano` as your editor when prompted)

```
crontab -e
```

Go to the bottom and add the line below to run `main_data_log.py` every 6 hours (starting midnight UTC)

```
* */6 * * * /home/ubuntu/data-eng-exercise/venv/bin/python /home/ubuntu/data-eng-exercise/application/server/main_data_log.py
```

To test `main_data_log.py` prior to CRON running it, run manually w/ your venv activated:

```commandline
python /home/ubuntu/data-eng-exercise/application/server/main_data_log.py
```

After each run, check the log file to see if everything ran properly.

```commandline
nano /home/ubuntu/data-eng-exercise/application/server/logs/data_log.txt
```

return to [TOC](#table-of-contents)

## Setup AWS Webapp Server

Follow the tutorial [here](https://www.digitalocean.com/community/tutorials/how-to-serve-flask-applications-with-uwsgi-and-nginx-on-ubuntu-20-04) to setup an AWS instance to serve a Flask app w/ Nginx and uWSGI. Clone this repo in the tutorial. Some of the final files are below.

***IMPORTANT NOTE:*** At the end of the tutorial, in order for the server to run, you'll need to change the `user` in the `nginx.conf` file to `ubuntu`:

```commandline
sudo nano /etc/nginx/nginx.conf
```

#### uWSGI Config - `application/server/bgdx_app.ini`

*See location in repo.*

#### Systemd unit file - `/etc/systemd/system/bgdx_app.service`

```commandline
[Unit]
Description=uWSGI instance to serve bgdx_app
After=network.target

[Service]
User=ubuntu
Group=www-data
WorkingDirectory=/home/ubuntu/data-eng-exercise/application/server
Environment="PATH=/home/ubuntu/data-eng-exercise/venv/bin"
ExecStart=/home/ubuntu/data-eng-exercise/venv/bin/uwsgi --ini bgdx_app.ini

[Install]
WantedBy=multi-user.target
```

#### Nginx config file - `/etc/nginx/sites-available/bgdx_app`

```commandline
server {
    listen 80;
    server_name bigdorksonly.io www.bigdorksonly.io;

    location / {
        include uwsgi_params;
        uwsgi_pass unix:/home/ubuntu/data-eng-exercise/application/server/bgdx_app.sock;
    }
}
```

### Declare local environment variables

```commandline
export SNOWFLAKE_USER=XXXXXXX
export SNOWFLAKE_PWD=XXXXXXX
export SNOWFLAKE_ACCOUNT=XXXXXXX.XXXXXX.aws
export SNOWFLAKE_DB=XXXXXXX
export FLIPSIDE_API_KEY=XXXXXXX
export CRYPTOWATCH_API=XXXXXXX
```

### Maintaining Server

To restart Nginx, stop the app, and restart it again:

```commandline
sudo systemctl restart nginx
sudo systemctl stop bgdx_app
sudo systemctl start bgdx_app
```

To debug server errors:

```commandline
sudo less /var/log/nginx/error.log    # checks the Nginx error logs.
sudo less /var/log/nginx/access.log   # checks the Nginx access logs.
sudo journalctl -u nginx              # checks the Nginx process logs.
sudo journalctl -u bgdx_app           # checks your Flask app’s uWSGI logs.
sudo journalctl -u -e bgdx_app        # checks your Flask app’s uWSGI logs (skip to end)
```

return to [TOC](#table-of-contents)

