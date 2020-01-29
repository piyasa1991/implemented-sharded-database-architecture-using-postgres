# Querying on sharded databases with the shared session
This is a tutorial on how to use sharded architecture using SQLalchemy and Python

## Case study
**Intro**
As our clients send us pretty large amounts of data, we store it on multiple servers. It is called sharding - you can read a bit more about it here.

Imagine we store hourly amount of installs by country on our machines.
Let’s say we have 3 shards of the databases, all with the same structure:

Table installs_by_country
(country varchar,
created_at datetime,
paid boolean,
installs int
)

Where the fields mean the following:
country - is a country as a text field
created_at - is a datetime field, rounded by an hour, so something like (‘2019-01-20 13:00:00’)
paid - means if the install was paid or organic
installs - amount of installs

**Task:**
Create 3 Postgres instances locally and insert the csv files to them
Build a pipeline that will return the amount of paid installs by country, which happened in May 2019.

## Requirements
- Python >=3.6
- pip
- Docker


## How to run

For Mac:
```
brew install pipenv

```
For windows:
```
pip install pipenv
```
Open the terminal and cd into the project directory
```
cd adjust_case_study_solution
```
Actiavte the environment for the project
```
pipenv install

pipenv shell

```
Run the following command for execute the task
```
chmod +x init.sh

./init.sh

```
