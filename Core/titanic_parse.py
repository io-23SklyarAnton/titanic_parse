import re
import time

import pandas as pd

from db.models_crud import TitanicDispatcher
from db import models


def split_name(name):
    """Splits the name into first and last name"""
    cleaned_name = re.sub(r',\s(Mr\.|Mrs\.|Miss\.) ', ',', name)
    cleaned_name = cleaned_name.split(',')
    first_name = cleaned_name[0]
    last_name = cleaned_name[1]
    return first_name, last_name


if __name__ == "__main__":
    url = "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv"
    titanic = models.TitanicModel()
    titanic.drop_tables()
    titanic.create_tables()
    titanic_dispatcher = TitanicDispatcher(titanic)
    while True:
        titanic_dispatcher.clear_tables()
        data = pd.read_csv(url)
        for i in range(0, len(data)):
            """person info"""
            first_name, last_name = split_name(data.loc[i, 'Name'])
            pclass, survived = int(data.loc[i, 'Pclass']), data.loc[i, 'Survived']
            age = int(data.loc[i, 'Age']) if not pd.isnull(data.loc[i, 'Age']) else None

            """ticket info"""
            ticket_number, fare, embarked = data.loc[i, 'Ticket'], data.loc[i, 'Fare'], data.loc[i, 'Embarked']
            cabin = data.loc[i, 'Cabin'] if not pd.isnull(data.loc[i, 'Cabin']) else None

            """passenger info"""
            passenger_id, sib_sp, par_ch = int(data.loc[i, 'PassengerId']), int(data.loc[i, 'SibSp']), int(
                data.loc[i, 'Parch'])

            """insert data"""
            person_id = titanic_dispatcher.insert_person(first_name, last_name, age, pclass, survived)
            ticket_id = titanic_dispatcher.insert_ticket(ticket_number, fare, embarked, cabin)
            titanic_dispatcher.commit()

            titanic_dispatcher.insert_passenger(passenger_id, sib_sp, par_ch, ticket_id, person_id)
            titanic_dispatcher.commit()

        time.sleep(3600)
