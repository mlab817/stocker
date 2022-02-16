import Conn


def get_companies():
    conn = Conn()

    conn.cursor.execute('select id, symbol from companies where active is TRUE order by id')
    companies = conn.cursor.fetchall()

    companies.sort()

    return companies