from datetime import datetime

from sqlalchemy import create_engine
# from config import DATABASE_URI_1,DATABASE_URI_2,DATABASE_URI_3
import config
from models import Base, InstallByCountry
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
import csv
from sqlalchemy.ext.horizontal_shard import ShardedSession

# db0 = create_engine(DATABASE_URI_1)
# db1 = create_engine(DATABASE_URI_2)
# db2 = create_engine(DATABASE_URI_3)

class DB(object):
    def __init__(self):
        db0 = create_engine(config.DATABASE_URI_1)
        db1 = create_engine(config.DATABASE_URI_2)
        db2 = create_engine(config.DATABASE_URI_3)
        self.dbs=(db0, db1, db2)
        # self.recreate_database()

    def recreate_database(self):
        for db in self.dbs:
            Base.metadata.drop_all(db)
            Base.metadata.create_all(db)

    #define sharding functions
    def shard_chooser(self,mapper, instance):
        """given an instance of mapped class, return its shard"""
        if instance.uid <=17268:
            return 0
        elif instance.uid >17268 and instance.uid <=(17268*2):
            return 1
        else:
            return 2

    def id_chooser(self,query, ident):
        """return ordered list of shards to search based on identifier"""
        return [0, 1, 2]

    def query_chooser(self,query):
        """generate a list of shards to search based on a query"""
        return [0, 1, 2]

    def get_session(self):
        create_session = sessionmaker(class_=ShardedSession)
        create_session.configure(shards={
            0:self.dbs[0],
            1:self.dbs[1],
            2:self.dbs[2]
        })
        create_session.configure(
                        shard_chooser=self.shard_chooser,
                        id_chooser=self.id_chooser,
                        query_chooser=self.query_chooser
                        )
        return create_session()
        
    def load_csv_into_table_old(self,filename, engine):
        print(filename)
        print(engine)
        Session = sessionmaker(bind=engine)
        session= Session()
        with open(filename,'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            next(csv_reader)
            buffer = []
            for row in csv_reader:
                buffer.append({
                    'uid': row[0],
                    'index': row[1],
                    'country': row[2],
                    'created_at': row[3],
                    'paid': row[4],
                    'installs': row[5]
                })
                # print(buffer)
                if len(buffer) % 10000 == 0:
                    session.bulk_insert_mappings(InstallByCountry,buffer)
                    buffer = []

            session.bulk_insert_mappings(InstallByCountry,buffer)
        session.commit()

    def load_csv_into_table(self,filenames):
        session=self.get_session()
        for filename in filenames:
            print('Inserting records from filename {}'.format(filename))
            with open(filename,'r') as csv_file:
                csv_reader = csv.reader(csv_file)
                next(csv_reader)
                buffer = []
                for row in csv_reader:
                    data={
                        'uid': int(row[0]),
                        'index': row[1],
                        'country': row[2],
                        'created_at': row[3],
                        'paid': row[4],
                        'installs': row[5]
                    }
                    install = InstallByCountry(**data)
                    buffer.append(install)
            for rec in buffer:
                session.add(rec)
            
        session.commit()
        print('Insertion completed..')
        session.close()

if __name__ == '__main__':
    filenames=['sharded_data/shard1.csv','sharded_data/shard2.csv',
    'sharded_data/shard3.csv']
    # recreate_database()
    dbobj=DB()
    dbobj.load_csv_into_table(filenames)
        

