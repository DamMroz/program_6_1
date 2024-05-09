import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    connection = None
    try:
        connection = sqlite3.connect(db_file)
        return connection
    except Error as error:
        print(error)
    return connection


def execute_sql_commands(connection, sql):
    try:
        c = connection.cursor()
        c.execute(sql)
    except Error as error:
        print(error)


def add_movie(connection, movie):
    sql_insert = '''INSERT INTO movie (name, genre, year)
                    VALUES(?,?,?)'''
    cur = connection.cursor()
    cur.execute(sql_insert, movie)
    connection.commit()
    return cur.lastrowid


def add_series(connection, series):
    sql_insert = '''INSERT INTO series (movie_id, name, genre, year)
                    VALUES(?,?,?,?)'''
    cur = connection.cursor()
    cur.execute(sql_insert, series)
    connection.commit()
    return cur.lastrowid


def update(connection, table, id, **kwargs):
    parameters = [f"{par} = ?" for par in kwargs]
    parameters = ", ".join(parameters)
    values = tuple(element for element in kwargs.values())
    values += (id, )
    sql_update = f''' UPDATE {table}
             SET {parameters}
             WHERE id = ?'''
    try:
        cur = connection.cursor()
        cur.execute(sql_update, values)
        connection.commit()
        print("OK")
    except sqlite3.OperationalError as error:
        print(error)


def delete_where(connection, table, **kwargs):
    query_list = []
    values = tuple()
    for x, y in kwargs.items():
        query_list.append(f"{x}=?")
        values += (y,)
    query_list_join = " AND ".join(query_list)
    sql_delete = f'DELETE FROM {table} WHERE {query_list_join}'
    cur = connection.cursor()
    cur.execute(sql_delete, values)
    connection.commit()
    print("Deleted")


def delete_all(connection, table):
    sql_delete = f'DELETE FROM {table}'
    cur = connection.cursor()
    cur.execute(sql_delete)
    connection.commit()
    print("Deleted")   


def select_all(connection, table):
    cur = connection.cursor()
    cur.execute(f"SELECT * FROM {table}")
    rows = cur.fetchall()
    return rows


def select_where(connection, table, **query):
    cur = connection.cursor()
    query_list = []
    values = ()
    for x, y in query.items():
        query_list.append(f"{x}=?")
        values += (y,)
    query_list_join = " AND ".join(query_list)
    cur.execute(f"SELECT * FROM {table} WHERE {query_list_join}", values)
    rows = cur.fetchall()
    return rows


if __name__ == "__main__":

    create_movie_sql = """
    CREATE TABLE IF NOT EXISTS movie (
        id integer PRIMARY KEY,
        name text NOT NULL,
        genre text,
        year text
    );
    """

    create_series_sql = """
    CREATE TABLE IF NOT EXISTS series (
        id integer PRIMARY KEY,
        movie_id integer NOT NULL,
        name text NOT NULL,
        genre text NOT NULL,
        year text NOT NULL,
        FOREIGN KEY (movie_id) REFERENCES movie (id)
    );
    """

    db_file = "my_database.db"

    connection = create_connection(db_file)
    if connection is not None:
        execute_sql_commands(connection, create_movie_sql)
        execute_sql_commands(connection, create_series_sql)
       
    movie = ("Matrix", "Sci-Fi", "1999")

    movie_id = add_movie(connection, movie)
   
    series = (
        movie_id,
        "Breaking Bad",
        "Drama",
        "2008")
    
    series_id = add_series(connection, series)
   
    print(movie_id, series_id)
    
    cur = connection.cursor()

    cur.execute("SELECT * FROM series")

    conn = create_connection("database.db")

    update(connection, "movie", 1, year=1999)

    print(select_all(connection, "series"))

    print(select_where(connection, "series", id=1,year=2008))

    delete_where(connection, "series", id=1,year=2008)

    delete_all(connection, "series")

    connection.commit()

    connection.close()