import os
import shutil
import time
import urllib
import urlparse
try:
    import simplejson as json
except ImportError:
    import json
from webob.dec import wsgify
from webob import Response
from webob import exc
from logdb import Database, ExpectationFailed

syncclient_filename = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    'syncclient.js')


class UserStorage(object):

    def __init__(self, dir, timer=time.time):
        self.dir = dir
        self.timer = timer

    def for_user(self, domain, username, bucket):
        dir = os.path.join(self.dir, urllib.quote(domain, ''), urllib.quote(username, ''), urllib.quote(bucket, ''))
        if not os.path.exists(dir):
            os.makedirs(dir)
        col_filename = os.path.join(dir, 'collection_id.txt')
        if not os.path.exists(col_filename):
            collection_id = str(int(self.timer() * 100) % 1000000)
            with open(col_filename, 'wb') as fp:
                fp.write(collection_id)
        else:
            with open(col_filename, 'rb') as fp:
                collection_id = fp.read()
        db_name = os.path.join(dir, 'database')
        db = Database(db_name)
        db.collection_id = collection_id
        return db

    def clear(self):
        shutil.rmtree(self.dir)
        os.mkdir(self.dir)


class Application(object):

    def __init__(self, storage=None, dir=None, mock_browserid=False,
                 remove_browserid=False,
                 include_syncclient=False,
                 secret_filename='/tmp/logdb-secret.txt'):
        if storage is None and dir:
            storage = UserStorage(dir)
        self.storage = storage
        self.mock_browserid = mock_browserid
        self.remove_browserid = remove_browserid
        self.include_syncclient = include_syncclient
        self._syncclient_app = None
        self._syncclient_mtime = None
        self._syncclient_app_url = None
        self._secret_filename = secret_filename

    def unauthorized(self, reason):
        return Response(
            status=401,
            content_type='text/plain',
            body=reason)

    def update_json(self, data, **kw):
        if isinstance(data, str):
            data = json.loads(data)
        data.update(kw)
        return data

    @wsgify
    def __call__(self, req):
        if self.include_syncclient and req.path_info == '/syncclient.js':
            return self.syncclient(req)
        if req.path_info == '/verify':
            return self.verify(req)
        self.annotate_auth(req)
        domain = req.path_info_pop()
        username = req.path_info_pop()
        bucket = req.path_info
        remote_user = req.environ.get('REMOTE_USER')
        if not remote_user:
            return self.unauthorized('No authentication provided')
        if '/' not in remote_user:
            raise Exception(
                'REMOTE_USER is not formatted as username/domain')
        remote_username, remote_domain = remote_user.split('/', 1)
        remote_domain = urlparse.urlsplit(remote_domain).netloc.split(':')[0]
        if remote_username != username:
            return self.unauthorized('Incorrect authentication provided (%r != %r)' % (remote_username, username))
        if remote_domain != domain:
            return self.unauthorized('Incorrect authentication provided: bad domain (%r != %r)' % (remote_domain, domain))
        if 'include' in req.GET and 'exclude' in req.GET:
            raise exc.HTTPBadRequest('You may only include one of "exclude" or "include"')
        db = self.storage.for_user(domain, username, bucket)
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
        return resp

    def post(self, req, db):
        try:
            data = req.json
        except ValueError:
            raise exc.HTTPBadRequest('POST must have valid JSON body')
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
        try:
            since = int(req.GET.get('since', 0))
        except ValueError:
            raise exc.HTTPBadRequest('Bad value since=%s' % req.GET['since'])
        try:
            limit = int(req.GET.get('limit', 0))
        except ValueError:
            raise exc.HTTPBadRequest('Bad value limit=%s' % req.GET['limit'])
        items = db.read(since)
        if limit:
            new_items = []
            for c, item in zip(xrange(limit), items):
                new_items.append(item)
            items = new_items
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

    def syncclient(self, req):
        if self._syncclient_app:
            mtime = os.path.getmtime(syncclient_filename)
            if mtime > self._syncclient_mtime:
                self._syncclient_app = None
            if req.application_url != self._syncclient_app_url:
                ## cache different app urls?
                self._syncclient_app = None
        if not self._syncclient_app:
            self._syncclient_mtime = os.path.getmtime(syncclient_filename)
            self._syncclient_app_url = req.application_url
            with open(syncclient_filename) as fp:
                content = fp.read()
            content = content.replace(
                'Sync.baseUrl = null',
                'Sync.baseUrl = %r' % req.application_url)
            self._syncclient_app = Response(
                content_type='text/javascript',
                conditional_response=True,
                body=content)
        return self._syncclient_app

    def verify(self, req):
        try:
            assertion = req.POST['assertion']
            audience = req.POST['audience']
        except KeyError, e:
            return exc.HTTPBadRequest('Missing key: %s' % e)
        r = urllib.urlopen(
            "https://browserid.org/verify",
            urllib.urlencode(
                dict(assertion=assertion, audience=audience)))
        r = json.loads(r.read())
        if r['status'] == 'okay':
            r['audience'] = audience
            static = json.dumps(r)
            static = sign(get_secret(self._secret_filename), static) + '.' + static
            r['auth'] = {'query': {'auth': static}}
        return Response(json=r)

    def annotate_auth(self, req):
        auth = req.GET.get('auth')
        print 'auth', repr(auth)
        if auth:
            sig, data = auth.split('.', 1)
            print 'sig', sig, 'expected', sign(get_secret(self._secret_filename), data)
            if sign(get_secret(self._secret_filename), data) == sig:
                data = json.loads(data)
                req.environ['REMOTE_USER'] = data['email'] + '/' + data['audience']
                print 'set', req.environ['REMOTE_USER']


def b64_encode(s):
    import base64
    return base64.urlsafe_b64encode(s).strip('=').strip()


def get_secret(filename):
    if not os.path.exists(filename):
        length = 10
        secret = b64_encode(os.urandom(length))
        with open(filename, 'wb') as fp:
            fp.write(secret)
    else:
        with open(filename, 'rb') as fp:
            secret = fp.read()
    return secret


def sign(secret, text):
    import hmac
    import hashlib
    return b64_encode(hmac.new(secret, text, hashlib.sha1).digest())
