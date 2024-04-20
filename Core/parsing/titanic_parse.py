import re
import time
from typing import List

import pandas as pd


class TitanicParser:
    def __init__(self):
        self.url = "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv"
        self.data = pd.read_csv(self.url)

    @staticmethod
    def split_name(name):
        """Splits the name into first and last name"""
        cleaned_name = re.sub(r',\s(Mr\.|Mrs\.|Miss\.) ', ',', name)
        cleaned_name = cleaned_name.split(',')
        return cleaned_name[0], cleaned_name[1]

    def get_all_persons(self) -> List[dict]:
        """Returns all persons in the csv file"""
        persons = []
        for i in range(0, len(self.data)):
            first_name, last_name = self.split_name(self.data.loc[i, 'Name'])
            person_class, survived = int(self.data.loc[i, 'Pclass']), self.data.loc[i, 'Survived']
            age = int(self.data.loc[i, 'Age']) if not pd.isnull(self.data.loc[i, 'Age']) else None
            persons.append({
                'first_name': first_name,
                'last_name': last_name,
                'age': age,
                'person_class': person_class,
                'survived': survived
            })
        return persons

    def get_all_tickets(self) -> List[dict]:
        """Returns all tickets in the csv file"""
        tickets = []
        ticket_numbers = set()
        for i in range(0, len(self.data)):
            ticket_number, fare, embarked = self.data.loc[i, 'Ticket'], self.data.loc[i, 'Fare'], self.data.loc[
                i, 'Embarked']
            cabin = self.data.loc[i, 'Cabin'] if not pd.isnull(self.data.loc[i, 'Cabin']) else None

            if ticket_number in ticket_numbers:
                continue

            ticket_numbers.add(ticket_number)
            tickets.append({
                'ticket_number': ticket_number,
                'fare': fare,
                'embarked': embarked,
                'cabin': cabin
            })
        return tickets

    def get_all_passengers(self) -> List[dict]:
        """Returns all passengers in the csv file"""
        passengers = []
        for i in range(0, len(self.data)):
            person_id, sib_sp, par_ch = int(self.data.loc[i, 'PassengerId']), int(self.data.loc[i, 'SibSp']), int(
                self.data.loc[i, 'Parch'])
            ticket_number = self.data.loc[i, 'Ticket']
            passengers.append({
                'sib_sp': sib_sp,
                'par_ch': par_ch,
                'ticket_number': ticket_number,
                'person_id': person_id
            })
        return passengers