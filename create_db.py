import sqlite3
import pandas as pd

def create_table(table_name, data):
    print('creating {} table'.format(table_name))
    df = pd.read_csv(data)
    df.to_sql(table_name, conn)

db_file = 'who_mortality.db'

conn = sqlite3.connect(db_file)

listoftables = conn.execute("""SELECT name FROM sqlite_master WHERE type='table'""").fetchall()
listoftables = [item[0] for item in listoftables]
print(listoftables)

# Create data table

if 'data' not in listoftables:
    create_table('data', 'who_mortality_data/Morticd10_part1')

    for i in range(2,6):
        print('csv {}'.format(i))
        data = 'who_mortality_data/Morticd10_part{}'.format(i)
        df = pd.read_csv(data)
        df.to_sql('temp', conn, if_exists='replace')
        sql = '''INSERT INTO data
                 SELECT * FROM temp'''
        conn.execute(sql)
        conn.commit()
        conn.execute('''DROP TABLE temp''')

# Create country codes table

if 'country_codes' not in listoftables:
    create_table('country_codes', 'who_mortality_data/country_codes')
    
# Create population table

if 'population' not in listoftables:
    create_table('population', 'who_mortality_data/pop')

conn.close()

