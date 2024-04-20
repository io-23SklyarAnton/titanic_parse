import time

from parsing.titanic_parse import TitanicParser
from db.models import drop_tables, create_tables
from db.models_crud import insert_persons, insert_tickets, insert_passengers
from threading import Thread

if __name__ == "__main__":
    while True:
        drop_tables()
        create_tables()

        titanic_parser = TitanicParser()

        thread_person = Thread(target=insert_persons, args=(titanic_parser.get_all_persons(),))
        thread_ticket = Thread(target=insert_tickets, args=(titanic_parser.get_all_tickets(),))
        thread_passenger = Thread(target=insert_passengers, args=(titanic_parser.get_all_passengers(),))

        [thread.start() for thread in [thread_person, thread_ticket, thread_passenger]]
        [thread.join() for thread in [thread_person, thread_ticket, thread_passenger]]
        time.sleep(3600)
