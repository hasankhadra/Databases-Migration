from pg_db import PG_SQL
from dynamoDB_db import DynamoDB
import decimal
from datetime import datetime

PG_instance = PG_SQL()
Dynamo_instance = DynamoDB()
"""
                                                                                                               
Retrieve all data from PG without inserting them in Dynamo                           h:mm:ss
(including table names, columns names, columns types, and all the data inside them): 0:00:00.182606

Upload all the data (including table names, columns names, columns types, and all the data inside them)
to DynamoDB (without the time of retrieving from PG): 0:06:23.048537
                                                                         h:mm:ss
Total time of migrating (retrieving from PG and uploading to Dynamo) is: 0:06:23.231143 

"""

class migrate:

    @staticmethod
    def start_timer():
        current_time = datetime.now()
        return current_time

    @staticmethod
    def stop_timer(begin_time):
        end_time = datetime.now()
        return end_time - begin_time

    @staticmethod
    def insert_tables():
        tables = PG_instance.get_table_names()

        responses = []

        for table in tables:
            attributes, types = PG_instance.get_attributes_names(table)
            responses.append(Dynamo_instance.create_table(table, attributes, types))

        return responses

    @staticmethod
    def insert_rows(num_of_rows=-1):
        tables = PG_instance.get_table_names()
        for table in tables:
            print(table)
            attributes, types = PG_instance.get_attributes_names(table)
            contents = PG_instance.select_all(table)

            if num_of_rows != -1 and len(contents) > num_of_rows:
                contents = contents[:num_of_rows]

            n = len(attributes)
            for row in contents:
                row = list(row)
                for x in range(n):
                    if types[x] == 'bytea':
                        types[x] = 'B'
                    if row[x] is None or row[x] == '':
                        row[x] = ''
                    else:
                        if types[x] == 'timestamp without time zone':
                            row[x] = row[x].strftime("%m/%d/%Y, %H:%M:%S")
                        if isinstance(row[x], list):
                            row[x] = row[x][0]
                        if isinstance(row[x], decimal.Decimal):
                            row[x] = str(row[x])
                Dynamo_instance.insert_item(table, types, attributes, row)


if __name__ == '__main__':

    begin_time = migrate.start_timer()

    migrate.insert_tables()
    migrate.insert_rows()

    time_consumed = migrate.stop_timer(begin_time)

    print(time_consumed)
