import os

import sqlalchemy
from sqlalchemy import create_engine, Index, inspect
from dotenv import load_dotenv

load_dotenv()


class TitanicModel:
    """Contains Person, Passenger, and Ticket tables for Titanic db"""

    def __init__(self):
        self.engine = create_engine(
            f'postgresql://{os.getenv("USER")}:{os.getenv("PASSWORD")}@{os.getenv("HOST")}/{os.getenv("DB_NAME")}',
            echo=True)
        self.metadata = sqlalchemy.MetaData()

        """The person table contains information about the passengers"""
        self.person_table = sqlalchemy.Table(
            'person', self.metadata,
            sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, autoincrement=True),
            sqlalchemy.Column('first_name', sqlalchemy.String),
            sqlalchemy.Column('last_name', sqlalchemy.String),
            sqlalchemy.Column('age', sqlalchemy.Integer, nullable=True),
            sqlalchemy.Column('person_class', sqlalchemy.String),
            sqlalchemy.Column('survived', sqlalchemy.Boolean)
        )

        """The ticket table contains information about the tickets purchased by the passengers"""
        self.ticket_table = sqlalchemy.Table(
            'ticket', self.metadata,
            sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column('ticket_number', sqlalchemy.String),
            sqlalchemy.Column('fare', sqlalchemy.Float),
            sqlalchemy.Column('embarked', sqlalchemy.String),
            sqlalchemy.Column('cabin', sqlalchemy.String, nullable=True),
        )

        """The passenger table contains information about the passengers and their tickets"""
        self.passenger_table = sqlalchemy.Table(
            'passenger', self.metadata,
            sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column('person_id', sqlalchemy.Integer, sqlalchemy.ForeignKey('person.id', ondelete='CASCADE')),
            sqlalchemy.Column('ticket_id', sqlalchemy.Integer, sqlalchemy.ForeignKey('ticket.id', ondelete='CASCADE')),
            sqlalchemy.Column('sib_sp', sqlalchemy.Integer),
            sqlalchemy.Column('par_ch', sqlalchemy.Integer),
        )

    def create_tables(self):
        self.metadata.create_all(self.engine)
        idx_first_name = Index('idx_first_name', self.person_table.c.first_name)
        idx_full_name = Index('idx_full_name', self.person_table.c.first_name, self.person_table.c.last_name)

        self.create_index_if_not_exists(idx_first_name)
        self.create_index_if_not_exists(idx_full_name)

    def create_index_if_not_exists(self, index):
        insp = inspect(self.engine)
        table_indexes = insp.get_indexes(index.table.name)
        if not any(idx['name'] == index.name for idx in table_indexes):
            index.create(self.engine)
