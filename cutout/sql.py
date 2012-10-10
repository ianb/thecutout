import MySQLdb


class MySQLStorage(object):

    def __init__(self, **kw):
        self.conn = MySQLdb.connect(**kw)
        cur = self.conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS data (
          id INT PRIMARY KEY AUTO_INCREMENT,
          scope VARCHAR(250) NOT NULL,
          value TEXT NOT NULL,
          INDEX (scope)
        )
        """)
        cur.close()

    def __call__(self, name):
        return MySQLScoped(self.conn, name)


class MySQLScoped(object):

    def __init__(self, conn, scope):
        self.conn = conn
        self.scope = scope

    def extend(self, datas):
        cur = self.conn.cursor()
        first = None
        for data in datas:
            cur.execute("""
            INSERT INTO data (scope, value) VALUES
            (%s, %s);
            """, (self.scope, data))
            if first is None:
                first = self.conn.insert_id()
        cur.close()
        return first

    def read(self, least):
        ## Note we don't do reverse order, seems unfair since it's
        ## incidental to the other implementation, not actually
        ## particularly useful.
        q = """SELECT id, value FROM data WHERE id > %s AND scope = %s"""
        params = (least, self.scope)
        cur = self.conn.cursor()
        cur.execute(q, params)
        return cur.fetchall()

    def clear(self):
        cur = self.conn.cursor()
        cur.execute("DELETE FROM data")
        cur.close()

    def length(self):
        cur = self.conn.cursor()
        cur.execute("""SELECT count(*) FROM data WHERE scope = %s""", (self.scope,))
        row = cur.fetchone()
        if not row:
            return 0
        return row[0]
