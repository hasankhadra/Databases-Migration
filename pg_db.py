import psycopg2
import decimal
from datetime import datetime

class PG_SQL:

    def init(self):
        conn = psycopg2.connect(database="dvdrental", user="postgres",
                                password="mddwikabdro", host="127.0.0.1", port="5432")
        crsr = conn.cursor()

        return conn, crsr

    def get_table_names(self):
        conn, crsr = self.init()
        crsr.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = "
                     "'BASE TABLE';")
        tables_tmp = crsr.fetchall()

        conn.commit()
        crsr.close()
        conn.close()

        tables = []
        for table in tables_tmp:
            tables.append(table[0])
        return tables

    def get_attributes_names(self, table_name):
        conn, crsr = self.init()
        crsr.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND "
                     f"table_name = %s;", [table_name])
        columns_tmp = crsr.fetchall()

        conn.commit()
        crsr.close()
        conn.close()

        columns = []
        types = []
        for column in columns_tmp:
            columns.append(column[0])
            types.append(column[1])

        return columns, types

    def select_all(self, name):
        conn, crsr = self.init()

        crsr.execute(f"SELECT * FROM {name};")
        ans = crsr.fetchall()

        conn.commit()
        crsr.close()
        conn.close()

        return ans

    def get_all_data(self):
        conn, crsr = self.init()

        tables = self.get_table_names()
        data = []

        for table in tables:
            data.append(self.select_all(table))
        return data

    def insert_into(self, table_name, Info: dict):
        """
        :param table_name: the name of the table we want to insert into
        :param Info: a dictionary of the information that want to be inserted for a specific row
        {'atrribute_name': new_value}
        :return: Insert a new row in the specified table
        """
        attr = ''
        keys = list(Info.keys())
        info_values = list(Info.values())

        for x in range(len(keys)):
            tmp = ''.join([keys[x], ", "])
            attr += tmp
        attr = attr[:-2]

        values = "%s, " * len(keys)
        values = values[:-2]

        conn, crsr = self.init()
        crsr.execute(f"INSERT INTO {table_name} ({attr}) VALUES ({values})", info_values)
        conn.commit()

        crsr.close()
        conn.close()

    def update(self, table_name, Id, info):
        """
        :param table_name: the name of the table we want to update
        :param Id: the primary key of the record we want to update
        :param Info: dictionary of the details we want to updated {'atrribute_name': new_value}
        """

        keys = list(info.keys())
        values = list(info.values())

        res = ''
        for x in range(len(keys)):
            tmp = ''.join([keys[x], " = %s, "])
            res += tmp
        res = res[:-2]

        conn, crsr = self.init()
        crsr.execute(f"UPDATE {table_name} SET {res} WHERE {table_name}_id = {Id}", values)
        conn.commit()

        crsr.close()
        conn.close()

    def delete(self, table_name, Id):
        conn, crsr = self.init()
        crsr.execute(f"DELETE FROM {table_name} WHERE {table_name}_id = %s", [Id])
        conn.commit()

        crsr.close()
        conn.close()

    def given_query(self):
        conn, crsr = self.init()

        crsr.execute("""EXPLAIN ANALYZE
                        select *, (select count(*) from rental r2, payment p2 where r2.rental_id
                        = p2.rental_id and p2.amount<p.amount) as count_smaller_pay from rental
                        r, payment p where r.rental_id = p.rental_id;""")
        ans = crsr.fetchall()
        conn.commit()
        crsr.close()
        conn.close()
        return ans


if __name__ == "__main__":
    PG_inst = PG_SQL()

    """
    Q1:
    PGSQL:
    cost=510.99..12062691.37
    time: 0:00:31.848403
    
    Q2:
    PGSQL:
    update on actor table: 0:00:00.009944 
    insert into actor table: 0:00:00.007929
    delete from actor table: 0:00:00.005629
    
    PG_inst.update('actor', 1, {'first_name': 'fdsa', 'last_name': 'fdsafdsa', 'last_update': datetime.now()})
    PG_inst.insert_into('actor', {'first_name': 'Hasan', 'last_name': 'Khadra', 'last_update': datetime.now()})
    PG_inst.delete('actor', 201)
    """
    # PG_inst.update('actor', 1, {'first_name': 'fdsa', 'last_name': 'fdsafdsa', 'last_update': datetime.now()})
    # PG_inst.insert_into('actor', {'first_name': 'Mohammad', 'last_name': 'Dwik', 'last_update': datetime.now()})
    PG_inst.delete('actor', 201)


    now = datetime.now()
    # PG_inst.given_query()
    now1 = datetime.now()

    print(now1 - now)



