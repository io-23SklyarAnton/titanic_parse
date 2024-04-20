import os

from sqlalchemy import create_engine
from dotenv import load_dotenv
from sqlalchemy.orm import sessionmaker, scoped_session, DeclarativeBase

from sqlalchemy import Column, Integer, String, Boolean, Float, ForeignKey
from sqlalchemy.orm import relationship

load_dotenv()

engine = create_engine(f'{os.getenv("DB_DRIVER")}://{os.getenv("DB_USER")}:{os.getenv("DB_PASS")}' +
                       f'@{os.getenv("DB_HOST")}:{os.getenv("DB_PORT")}/{os.getenv("DB_NAME")}',
                       pool_size=100,
                       max_overflow=100,
                       )


def read_session(func):
    def inner(*args, **kwargs):
        session_factory = sessionmaker(engine)
        session = scoped_session(session_factory)
        result = func(session, *args, **kwargs)
        session.close()
        return result

    return inner


def write_session(func):
    def inner(*args, **kwargs):
        session_factory = sessionmaker(engine)
        session = scoped_session(session_factory)
        result = func(session, *args, **kwargs)
        session.commit()
        session.close()
        return result

    return inner


class Base(DeclarativeBase):
    pass


class Person(Base):
    __tablename__ = 'person'

    id = Column(Integer, primary_key=True, autoincrement=True)
    first_name = Column(String)
    last_name = Column(String)
    age = Column(Integer, nullable=True)
    person_class = Column(String)
    survived = Column(Boolean)

    passengers = relationship("Passenger", back_populates="person")


class Ticket(Base):
    __tablename__ = 'ticket'

    ticket_number = Column(String, primary_key=True)
    fare = Column(Float)
    cabin = Column(String, nullable=True)
    embarked = Column(String)

    passengers = relationship("Passenger", back_populates="ticket")


class Passenger(Base):
    __tablename__ = 'passenger'

    id = Column(Integer, primary_key=True, autoincrement=True)
    person_id = Column(Integer, ForeignKey('person.id', ondelete='CASCADE'))
    ticket_number = Column(String, ForeignKey('ticket.ticket_number', ondelete='CASCADE'))
    sib_sp = Column(Integer)
    par_ch = Column(Integer)

    person = relationship("Person", back_populates="passengers")
    ticket = relationship("Ticket", back_populates="passengers")


def create_tables():
    Base.metadata.create_all(engine)


def drop_tables():
    Base.metadata.drop_all(engine)
