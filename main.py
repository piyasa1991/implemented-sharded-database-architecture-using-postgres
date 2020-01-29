from db import DB
from models import InstallByCountry
from sqlalchemy import func

filenames=['sharded_data/shard1.csv','sharded_data/shard2.csv',
    'sharded_data/shard3.csv']

dbobj=DB()
dbobj.recreate_database()
dbobj.load_csv_into_table(filenames)

session = dbobj.get_session()
print('Querying from the database..')
q = session.query(InstallByCountry.country, 
        func.sum(InstallByCountry.installs)).filter(
            InstallByCountry.paid=='True', 
            InstallByCountry.created_at >= '2019-05-01',
            InstallByCountry.created_at <= '2019-05-31').group_by(
                InstallByCountry.country)

unique_countries=set([record[0] for record in q.all()])
unique_countries= dict((ele,0)for ele in unique_countries)  

for d in q.all():
    key=d[0]
    unique_countries[key] += d[1]
print('Amount of paid installs by country which happened in May 2019:\n',unique_countries)