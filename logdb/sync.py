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
