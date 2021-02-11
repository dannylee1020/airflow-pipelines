import requests
from urllib.parse import urlencode
import psycopg2
import psycopg2.extras
from decouple import config

api_key = config('API_KEY')
region = 'Los Angeles'
db_table = 'dup_la_traffic'


def replace(output):
    cnt = 0
    lst = []
    while cnt < len(output):
        if 'E/O' in output[cnt][0]:
            s = output[cnt][0].replace('E/O','AND')
        elif 'S/O' in output[cnt][0]:
            s = output[cnt][0].replace('S/O','AND')
        elif 'W/O' in output[cnt][0]:
            s = output[cnt][0].replace('W/O','AND')
        else:
            s = output[cnt][0].replace('N/O','AND')

        new_add = {'address':s, 'old_address':output[cnt][0]}
        lst.append(new_add)
        cnt+=1
    return lst



def get_location():
    connection = psycopg2.connect(
    host = 'localhost',
    user = 'dhyungseoklee',
    dbname = 'la_traffic',
    password = None
    )

# get address from database
    curr = connection.cursor()
    sql = '''
        select
            location
        from %s 
    ''' %(db_table)

    curr.execute(sql)
    res = curr.fetchall()

# convert addresses 
    address_from_db = replace(res)

# get location from google api
    values = []
    for i in range(0,len(address_from_db)):
        session = requests.Session()
        response = session.get('https://maps.googleapis.com/maps/api/geocode/json?'+ urlencode({
            'address': address_from_db[i],
            'region': region,
            'key':api_key
        }))

        data = response.json()
        try:
            lat = data['results'][0]['geometry']['location']['lat']
            long = data['results'][0]['geometry']['location']['lng']
        except:
            lat = 0
            long = 0

        location = {
            'old_address':res[i][0],
            'lat':lat,
            'long':long
        }
        values.append(location)

# add data to the db table
    create_column_sql = '''
        ALTER TABLE %s
        ADD COLUMN IF NOT EXISTS lat DECIMAL,
        ADD COLUMN IF NOT EXISTS long DECIMAL
        '''  %(db_table)

    add_location_sql = '''
        UPDATE dup_la_traffic
        SET
            lat = %(lat)s,
            long = %(long)s 
        WHERE location = %(old_address)s 
        ''' 


    curr.execute(create_column_sql)
    curr.executemany(add_location_sql, values)
    connection.commit()
    connection.close()


if __name__ == '__main__':
    get_location()