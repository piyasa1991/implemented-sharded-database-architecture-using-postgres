from db import DB
from models import InstallByCountry
from sqlalchemy import func

# filenames from which data needs to be imported
filenames=['sharded_data/shard1.csv','sharded_data/shard2.csv',
    'sharded_data/shard3.csv']

# create an object of the database
dbobj=DB()

# recreate the database with the tables.
dbobj.recreate_database()

# import the csv into the tables.
dbobj.load_csv_into_table(filenames)

# create seperate session for querying the database
session = dbobj.get_session()
print('Querying from the database..')

# constructing the query
# This is similar to SQL expression
# SELECT country, SUM(installs) FROM installs_by_country
# WHERE paid='True' AND created_at >= '2019-05-01' 
# AND created_at <= '2019-05-31'

# this should return the amount of paid installs by country, 
# which happened in May 2019.
q = session.query(InstallByCountry.country, 
        func.sum(InstallByCountry.installs)).filter(
            InstallByCountry.paid=='True', 
            InstallByCountry.created_at >= '2019-05-01',
            InstallByCountry.created_at <= '2019-05-31').group_by(
                InstallByCountry.country)

# determinig the names of the unique countries from the result of the query
unique_countries=set([record[0] for record in q.all()])
unique_countries= dict((ele,0)for ele in unique_countries)  

# aggregating the results of the query returned by the shards into one.
for d in q.all():
    key=d[0]
    unique_countries[key] += d[1]
print('Amount of paid installs by country which happened in May 2019:\n',unique_countries)