import psycopg2


def create_db():

    dbname = 'la_traffic'
    user = 'dhyungseoklee'
    table_name = 'staging_la_traffic'

    connection = psycopg2.connect(
        host = 'localhost',
        user = user,
        database = dbname,
        password = 'None'
    )

    connection.autocommit = True
    curr = connection.cursor()

    create_table = ''' 
    DROP TABLE IF EXISTS %s ;
    CREATE UNLOGGED TABLE IF NOT EXISTS %s
        (
            location              TEXT,
            count_date            DATE,
            dayofweek             TEXT,
            total_count           INTEGER
        )
        ''' % (table_name, table_name)

    curr.execute(create_table)
    connection.close()



if __name__ == '__main__':
    create_db()


    

