# message_queue
Distributed Systems :- Implementation of a Distributed Message Queue

Prerequisites:

python3
pip
postgres

python modules:
psycopg2-binary
requests


Installation Instructions:

1. postgres:
sudo apt install postgresql postgresql-contrib
sudo apt install postgresql

2. Configure Database:
sudo -u postgres psql
ALTER USER postgres with PASSWORD 'test123';
create database d_queue;
\q --> to quit

3. pip install psycopg2-binary


How to run:

HTTP Server:
python3 main.py

Tests:
python3 test.py


How to use Producer and Consumer libraries:
copy src/producer_client.py and consumer_client.py as necessary into your working directory and import the classes.
