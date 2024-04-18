from sqlalchemy.orm import sessionmaker
from .models import TitanicModel


class TitanicDispatcher:
    """Inserts data into the Titanic db"""

    def __init__(self, titanic_instance: TitanicModel):
        self.titanic = titanic_instance
        self.session = sessionmaker(bind=self.titanic.engine)()

    def insert_person(self, first_name: str, last_name: str, age: int, person_class: int, survived: bool):
        result = self.session.execute(
            self.titanic.person_table.insert().values(first_name=first_name, last_name=last_name, age=age,
                                                      person_class=person_class, survived=survived))
        return result.inserted_primary_key[0]

    def insert_ticket(self, ticket_number: str, fare: float, embarked: str, cabin: str):
        result = self.session.execute(
            self.titanic.ticket_table.insert().values(ticket_number=ticket_number, fare=fare,
                                                      embarked=embarked, cabin=cabin))
        return result.inserted_primary_key[0]

    def insert_passenger(self, passenger_id: int, sib_sp: int, par_ch: int, ticket_id: int, person_id: int):
        self.session.execute(
            self.titanic.passenger_table.insert().values(id=passenger_id, sib_sp=sib_sp, par_ch=par_ch,
                                                         ticket_id=ticket_id, person_id=person_id))

    def clear_tables(self):
        self.session.execute(self.titanic.person_table.delete())
        self.session.execute(self.titanic.ticket_table.delete())
        self.session.execute(self.titanic.passenger_table.delete())

    def commit(self):
        self.session.commit()
