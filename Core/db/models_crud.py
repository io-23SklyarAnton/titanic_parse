from typing import List
from sqlalchemy.orm import Session
from .models import Person, Ticket, Passenger, write_session
from sqlalchemy import insert


def __insert_objects(session: Session, table: [Person | Passenger | Ticket], objects: List[dict]):
    q = insert(table).values(objects)
    session.execute(q)


@write_session
def insert_persons(session: Session, persons: List[dict]):
    __insert_objects(session, Person, persons)


@write_session
def insert_tickets(session: Session, tickets: List[dict]):
    __insert_objects(session, Ticket, tickets)


@write_session
def insert_passengers(session: Session, passengers: List[dict]):
    __insert_objects(session, Passenger, passengers)
