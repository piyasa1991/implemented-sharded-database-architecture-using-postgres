from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Boolean
from sqlalchemy.dialects.postgresql import TIMESTAMP


Base = declarative_base()

class InstallByCountry(Base):
    __tablename__ = 'installs_by_country'
    uid = Column(Integer)
    index = Column(Integer, primary_key=True)
    country = Column(String)
    created_at = Column(TIMESTAMP)
    paid = Column(String)
    installs = Column(Integer)

    def __repr__(self):
        return "InstallByCountry(index='{}', country='{}', created_at={}, paid={}, installs={})" \
            .format(self.index, self.country, self.created_at, self.paid, self.installs)