import os
import time
import urllib
import simplejson as json
import hmac
import hashlib
from webob.dec import wsgify
from webob import Response
from webob import exc
from logdb import Database, ExpectationFailed


_secret = None


def get_secret():
    global _secret
    if _secret is not None:
        return _secret
    fn = '/tmp/sync-secret.txt'
    if os.path.exists(fn):
        with open(fn, 'rb') as fp:
            _secret = fp.read().strip()
    else:
        with open(fn, 'wb') as fp:
            _secret = os.urandom(20).encode('base64').strip().strip('=')
            fp.write(_secret)
    return _secret


class UserStorage(object):

    def __init__(self, dir, timer=time.time):
        self.dir = dir
        self.timer = timer

    def for_user(self, username, domain):
        dir = os.path.join(self.dir, urllib.quote(domain, ''))
        if not os.path.exists(dir):
            os.makedirs(dir)
        filename = os.path.join(dir, urllib.quote(username, ''))
        col_filename = filename + '.collection_id'
        if not os.path.exists(col_filename):
            collection_id = str(int(self.timer()) % 100000)
            with open(col_filename, 'wb') as fp:
                fp.write(collection_id)
        else:
            with open(col_filename, 'rb') as fp:
                collection_id = fp.read()
        filename = filename + '.db'
        db = Database(filename)
        db.collection_id = collection_id
        return db


class Application(object):

    def __init__(self, storage, mock_browserid=False):
        self.storage = storage
        self.mock_browserid = mock_browserid

    def update_json(self, data, **kw):
        if isinstance(data, str):
            data = json.loads(data)
        data.update(kw)
        return data

    def check_auth(self, auth, domain, username):
        try:
            type, data = auth.split(None, 1)
        except ValueError:
            raise exc.HTTPBadRequest("Bad Authorization header")
        type = type.lower()
        if type not in ('browserid', 'synctoken'):
            raise exc.HTTPBadRequest("Bad Authorization type")
        if type == 'browserid':
            return self.check_browserid(data, domain, username)
        else:
            return self.check_synctoken(data, domain, username)

    def check_browserid(self, data, domain, username):
        if domain.startswith('https:'):
            audience = 'https://%s' % domain
        else:
            audience = 'http://%s' % domain
        resp = urllib.urlopen(
            'https://browserid.org/verify', 'assertion=%s&audience=http://%s' % (
                urllib.quote(data), urllib.quote(audience)))
        resp = json.loads(resp.read())
        if not self.mock_browserid:
            if resp['status'] != 'okay':
                raise exc.HTTPAuthorizationRequired("Invalid assertion")
            if resp['email'] != username:
                raise exc.HTTPAuthorizationRequired("Invalid user in assertion")
        return {"X-Set-Authorization": "SyncToken %s" % self.sign_auth(domain, username)}

    def check_synctoken(self, data, domain, username):
        nonce, rest = data.split(':', 1)
        expected = self.sign_auth(domain, username, nonce)
        if expected != data:
            raise exc.HTTPAuthorizationRequired("Invalid SyncToken")
        return None

    def sign_auth(self, domain, username, nonce=None):
        if not nonce:
            nonce = os.urandom(10).encode('base64').strip().strip('=')[:10]
        result = hmac.new(nonce, '%s %s' % (domain, username), hashlib.sha1).digest()
        result = result.encode('base64').strip().strip('=')
        return '%s:%s' % (nonce, result)

    @wsgify
    def __call__(self, req):
        domain = req.path_info_pop()
        username = req.path_info_pop()
        if 'include' in req.GET and 'exclude' in req.GET:
            raise exc.HTTPBadRequest('You may only include one of "exclude" or "include"')
        auth = req.headers.get('Authorization')
        if not auth:
            raise exc.HTTPAuthorizationRequired
        auth_headers = self.check_auth(auth, domain, username)
        db = self.storage.for_user(username, domain)
        collection_id = req.GET.get('collection_id')
        if collection_id is not None and collection_id != db.collection_id:
            req.GET.since = '0'
            resp_data = self.get(req, db)
            resp_data = self.update_json(
                resp_data, collection_changed=True,
                collection_id=db.collection_id)
        elif req.method == 'POST':
            resp_data = self.post(req, db)
        else:
            resp_data = self.get(req, db)
        if 'collection_id' not in req.GET:
            resp_data = self.update_json(resp_data, collection_id=db.collection_id)
        if not isinstance(resp_data, str):
            resp_data = json.dumps(resp_data)
        resp = Response(resp_data, content_type='application/json')
        if auth_headers:
            resp.headers.update(auth_headers)
        return resp

    def post(self, req, db):
        data = json.loads(req.body)
        data_encoded = [json.dumps(i) for i in data]
        since = int(req.GET.get('since', 0))
        counter = None
        try:
            counter = db.extend(data_encoded, expect_latest=since)
        except ExpectationFailed:
            pass
        if counter is None and 'include' in req.GET or 'exclude' in req.GET:
            failed = False
            for i in range(3):
                # Try up to three times to do this post, when there are soft failures.
                includes = req.GET.getall('include')
                excludes = req.GET.getall('exclude')
                for item_counter, item in db.read(since):
                    item = json.loads(item)
                    if includes and item['type'] in includes:
                        # Actual failure
                        failed = True
                        break
                    if excludes and item['type'] not in excludes:
                        failed = True
                        break
                    since = item_counter
                if failed:
                    break
                try:
                    counter = db.extend(data_encoded, expect_latest=since)
                    break
                except ExpectationFailed:
                    pass
        if counter is None:
            resp_data = self.get(req, db)
            resp_data = self.update_json(resp_data, invalid_since=True)
            return resp_data
        counters = [counter + index for index in range(len(data))]
        return dict(object_counters=counters)

    def get(self, req, db):
        since = int(req.GET.get('since', 0))
        limit = int(req.GET.get('limit', 0))
        items = db.read(since)
        if limit:
            items = iter(items)
            items = [items.next() for i in range(limit)]
        if 'include' in req.GET or 'exclude' in req.GET:
            return self.get_filtered(req, db, items)
        result = '{"objects":[%s]}' % (
            ','.join('[%i,%s]' % (count, item)
                     for count, item in items))
        return result

    def get_filtered(self, req, db, items):
        include = req.GET.getall('include')
        exclude = req.GET.getall('exclude')
        objects = []
        for count, item in items:
            item = json.loads(item)
            if include and item['type'] not in include:
                continue
            if exclude and item['type'] in exclude:
                continue
            objects.append((count, item))
        return dict(objects=objects)
