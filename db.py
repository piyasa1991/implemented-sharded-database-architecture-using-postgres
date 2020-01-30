from datetime import datetime

from sqlalchemy import create_engine
import config
from models import Base, InstallByCountry
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
import csv
from sqlalchemy.ext.horizontal_shard import ShardedSession

class DB(object):
    def __init__(self):
        db0 = create_engine(config.DATABASE_URI_1)
        db1 = create_engine(config.DATABASE_URI_2)
        db2 = create_engine(config.DATABASE_URI_3)
        # contains all the 3 instances of the database
        self.dbs=(db0, db1, db2) 

    def recreate_database(self):
        """
        Creates the tables in the database
        """
        for db in self.dbs:
            Base.metadata.drop_all(db)
            Base.metadata.create_all(db)

    #define sharding functions
    def shard_chooser(self,mapper, instance):
        """ Returns the shard of the instance
            The shard of an instance is determined by the uid of the 
            instance. For eg. if the uid of the instance is 17261
            then it will go to the first database i.e. db0, if it is more than 17268
            and less than 35436 then it resides in db1, else db2.
        
        Arguments:
            mapper {Mapper} -- Mapper of the instance
            instance {[type]} -- instance
        
        Returns:
            int -- Shard id
        """
        
        if instance.uid <=17268:
            return 0
        elif instance.uid >17268 and instance.uid <=(17268*2):
            return 1
        else:
            return 2

    def id_chooser(self,query, ident):
        """
        Return ordered list of shards to search based on identifier
        
        Arguments:
            query {[type]} -- SQL clause
            ident {[type]} -- [description]
        
        Returns:
            list -- Ordered list of shard id
        """
        return [0, 1, 2]

    def query_chooser(self,query):
        """
        Generate a list of shards to search based on a query
        
        Arguments:
            query {[type]} -- [description]
        
        Returns:
            list -- order list of shard id
        """     
        return [0, 1, 2]

    def get_session(self):
        """ Configures and creates a shared session 
        for the engines
        
        Returns:
            Session -- returns a Session object from which the 
                        tables need to be populated
        """

        # create a session
        create_session = sessionmaker(class_=ShardedSession)

        # configure the session with the shard lookup
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


    def load_csv_into_table(self,filenames):
        """
        Imports the CSV file into the database tables
        
        Arguments:
            filenames {list} -- provide the list of filenames
        """

        # get the session object
        session=self.get_session()

        # for each file, run through all the records and populate into the
        # InstallByCountry model.
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
            # add the objects of the model to the session
            for rec in buffer:
                session.add(rec)

        # commit the changes so that it gets reflected into the database   
        session.commit()
        print('Insertion completed..')
        # close the session
        session.close()

