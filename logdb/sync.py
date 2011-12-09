import os
import urllib
import simplejson as json
from webob.dec import wsgify
from webob import Response
from logdb import Database


class UserStorage(object):

    def __init__(self, dir):
        self.dir = dir

    def for_user(self, username):
        filename = os.path.join(self.dir, urllib.quote(username, ''))
        db = Database(filename)
        return db


class MySQLStorage(object):

    def __init__(self, **kw):
        import MySQLdb
        self.conn = MySQLdb.connect(**kw)
        cur = self.conn.cursor()
        cur.execute("""
        CREATE TABLE data IF NOT EXISTS (
          id INT AUTO_INCREMENT,
          username VARCHAR(250),
          value TEXT
        )
        """)
        cur.close()

    def for_user(self, username):
        return MySQLConnection(self.conn, username)


class MySQLConnection(object):

    def __init__(self, conn, username):
        self.conn = conn
        self.username = username

    def extend(self, datas):
        cur = self.conn.cursor()
        first = None
        for data in datas:
            cur.execute("""
            INSERT INTO data (username, value) VALUES
            (%s, %s);
            """, (self.username, data))
            if first is None:
                first = cur.lastrowinsert
        cur.close()
        return first

    def read(self, id, length=-1):
        q = """SELECT id, value FROM data WHERE id > %s AND username = %s"""
        params = (id, self.username)
        if length >= 0:
            q += " ORDER BY id LIMIT %s"
            params += (length,)
        cur = self.conn.cursor()
        cur.execute(q, params)
        return cur.fetchall()


class Application(object):

    def __init__(self, storage):
        self.storage = storage

    @wsgify
    def __call__(self, req):
        username = req.path_info_pop()
        db = self.storage.for_user(username)
        if req.method == 'POST':
            data = json.loads(req.body)
            counter = db.extend([json.dumps(i) for i in data])
            last = counter + len(data) - 1
            return Response(body='{"last":%i}' % last,
                            content_type='application/json')
        else:
            since = int(req.GET.get('since', 0))
            result = []
            latest = 0
            for index, data in db.read(since):
                result.append(data)
                latest = index
            return Response(body='{"until":%i,"items":[%s]}' % (latest, ','.join(result)),
                            content_type='application/json')
