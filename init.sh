# /bin/sh
# create 3 instances of the postgres locally
docker-compose up -d
sleep 10s
echo 'Docker has initialized 3 instances of Postgress..'
# run the main file to build the pipeline, populate the database
# and perform the query
python main.py