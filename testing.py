from dynamoDB_db import DynamoDB
import random
from datetime import datetime
from pg_db import PG_SQL
from migrate import migrate

Dynamo_inst = DynamoDB()
PG_inst = PG_SQL()


def test_show():
    l = Dynamo_inst.list_tables()
    print(l)
    x = Dynamo_inst.get_items(l[random.randint(0, len(l))])
    for i in range(min(10, len(x))):
        print(x[i])

def run_query():
    Dynamo_inst.given_query()

def insert_pg():
    PG_inst.insert_into('actor', {'first_name': 'Hasan', 'last_name': 'Khadra', 'last_update': datetime.now()})


def update_pg():
    PG_inst.update('actor', 1, {'first_name': 'fdsa', 'last_name': 'fdsafdsa', 'last_update': datetime.now()})


def delete_pg():
    PG_inst.delete('actor', 201)

def reset():
    Dynamo_inst.reset()

def migrate_data():
    begin_time = migrate.start_timer()

    migrate.insert_tables()
    migrate.insert_rows(100)

    time_consumed = migrate.stop_timer(begin_time)

    print(time_consumed)


if __name__ == "__main__":
    run_query()