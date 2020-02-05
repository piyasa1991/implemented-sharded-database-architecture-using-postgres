# Querying on sharded databases with the shared session

## Requirements
- Python >=3.6
- pip3 >=19.3.1
- Docker

## Solution:
Implemented a sharded PostgresSQL architecture and then queried on such 
architecture. Multiple engines were using the shared session.
This architecture resulted in faster read performance.

## How to run

For Mac:
```
brew install pipenv

```
For windows and Linux:
```
pip3 install pipenv
```

Open the terminal and browse into the project directory
```

cd implemented-sharded-database-architecture-using-postgres
```
Actiavte the environment for the project
```
pipenv install

pipenv shell

```
Give permission for the shell script
```
chmod +x init.sh

```
Run the shell script to run the pipeline
```
./init.sh
```

