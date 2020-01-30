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
