# /bin/sh
docker-compose up -d
sleep 10s
echo 'Docker has initialized 3 instances of Postgress..'
python main.py