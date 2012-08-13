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
from logdb import int_encoding


syncclient_filename = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    'syncclient.js')


class StorageDeprecated(Exception):
    pass


class UserStorage(object):

    def __init__(self, dir, timer=time.time):
        self.dir = dir
        self.timer = timer

    def for_user(self, domain, username, bucket):
        dir = os.path.join(self.dir, urllib.quote(domain, ''), urllib.quote(username, ''), urllib.quote(bucket, ''))
        return Storage(dir=dir, timer=self.timer)

    def clear(self):
        shutil.rmtree(self.dir)
        os.mkdir(self.dir)

    def all_dbs(self):
        result = []
        for dirpath, dirnames, filenames in os.walk(self.dir):
            if 'collection_id.txt' in filenames:
                assert dirpath.startswith(self.dir)
                dirpath = dirpath[len(self.dir):].strip(os.path.sep)
                parts = dirpath.split(os.path.sep)
                assert len(parts) == 3, 'Odd parts: %r' % parts
                result.append((
                        urllib.unquote(parts[0]),
                        urllib.unquote(parts[1]),
                        urllib.unquote(parts[2])))
        return result

    @property
    def is_disabled(self):
        return os.path.exists(os.path.join(self.dir, 'disabled'))

    def disable(self):
        with open(os.path.join(self.dir, 'disabled'), 'wb') as fp:
            fp.write('1')


class Storage(object):

    def __init__(self, dir, timer=time.time):
        self.dir = dir
        if not os.path.exists(dir):
            os.makedirs(dir)
        self.timer = timer
        self._collection_id = None

    @property
    def collection_id(self):
        if self._collection_id is not None:
            return self._collection_id
        col_filename = os.path.join(self.dir, 'collection_id.txt')
        if not os.path.exists(col_filename):
            collection_id = '%06i' % (int(self.timer() * 100) % (10 ** 6))
            self.set_collection_id(collection_id)
        else:
            with open(col_filename, 'rb') as fp:
                collection_id = fp.read()
        self._collection_id = collection_id
        return collection_id

    @property
    def has_collection_id(self):
        col_filename = os.path.join(self.dir, 'collection_id.txt')
        return os.path.exists(col_filename)

    def clear(self):
        shutil.rmtree(self.dir)

    @property
    def is_deprecated(self):
        return os.path.exists(os.path.join(self.dir, 'deprecated'))

    @property
    def db(self):
        db_name = os.path.join(self.dir, 'database')
        if self.is_deprecated:
            raise StorageDeprecated()
        db = Database(db_name)
        return db

    @property
    def deprecated_db(self):
        db_name = os.path.join(self.dir, 'deprecated')
        if not os.path.exists(db_name):
            raise IOError("File does not exist: %r" % db_name)
        db = Database(db_name)
        return db

    @property
    def queue_db(self):
        db_name = os.path.join(self.dir, 'queue')
        db = Database(db_name)
        return db

    @property
    def has_queue(self):
        return os.path.exists(os.path.join(self.dir, 'queue'))

    @property
    def empty(self):
        return (not self.is_deprecated and not self.has_queue and
                (not os.path.exists(os.path.join(self.dir, 'database.index'))
                 or os.path.getsize(os.path.join(self.dir, 'database.index')) == 12))

    def set_collection_id(self, collection_id):
        col_filename = os.path.join(self.dir, 'collection_id.txt')
        with open(col_filename, 'wb') as fp:
            fp.write(collection_id)
        self._collection_id = collection_id

    def deprecate(self):
        if self.is_deprecated:
            return
        db_name = os.path.join(self.dir, 'database')
        os.rename(db_name, os.path.join(self.dir, 'deprecated'))
        os.rename(db_name + '.index', os.path.join(self.dir, 'deprecated.index'))

    def encode_db(self, until=None):
        collection_id = self.collection_id.encode('ascii')
        if self.is_deprecated:
            db = self.deprecated_db
        else:
            db = self.db
        index_pos, data_pos = db.get_file_positions(until)
        return EncodedIterator(collection_id,
                               db.index_filename, index_pos,
                               db.data_filename, data_pos)

    def decode_db(self, fp, append_queue=False):
        (length,) = int_encoding.unpack(fp.read(4))
        collection_id = fp.read(length)
        col_filename = os.path.join(self.dir, 'new_collection_id.txt')
        with open(col_filename, 'wb') as col_fp:
            col_fp.write(collection_id)
        (length,) = int_encoding.unpack(fp.read(4))
        db_name = os.path.join(self.dir, 'new_database')
        ## FIXME: should lock queues here
        queue_filename = os.path.join(self.dir, 'queue')
        with open(db_name + '.index', 'wb') as new_fp:
            self._copy_chunked(fp, new_fp, length)
            if append_queue and os.path.exists(queue_filename + '.index'):
                with open(queue_filename + '.index', 'rb') as copy_fp:
                    ## FIXME: chunk
                    new_fp.write(copy_fp.read())
        (length,) = int_encoding.unpack(fp.read(4))
        with open(db_name, 'wb') as new_fp:
            self._copy_chunked(fp, new_fp, length)
            if append_queue and os.path.exists(queue_filename):
                with open(queue_filename, 'rb') as copy_fp:
                    ## FIXME: chunk
                    new_fp.write(copy_fp.read())
        for name in 'new_collection_id.txt', 'new_database.index', 'new_database':
            os.rename(os.path.join(self.dir, name),
                      os.path.join(self.dir, name[4:]))
        if append_queue:
            ## FIXME: also not atomic:
            for name in 'queue', 'queue.index':
                name = os.path.join(self.dir, name)
                if os.path.exists(name):
                    os.unlink(name)

    def _copy_chunked(self, old, new, length, chunk=4000 * 1024):
        while length > 0:
            chunk = old.read(min(length, chunk))
            length -= len(chunk)
            new.write(chunk)


class EncodedIterator(object):
    """An iterator for the result of db.encode_db()"""

    def __init__(self, collection_id, index_name, index_length, db_name, db_length, chunk=4000 * 1024):
        self.collection_id = collection_id
        self.db_name = db_name
        self.db_length = db_length
        self.index_name = index_name
        self.index_length = index_length
        self.chunk = chunk
        self.length = (
            4 + len(collection_id)
            + 4 + self.index_length
            + 4 + self.db_length)

    def __iter__(self):
        yield int_encoding.pack(len(self.collection_id))
        yield self.collection_id
        yield int_encoding.pack(self.index_length)
        left = self.index_length
        with open(self.index_name, 'rb') as fp:
            while left > 0:
                chunk = fp.read(min(self.chunk, left))
                left -= len(chunk)
                yield chunk
        yield int_encoding.pack(self.db_length)
        left = self.db_length
        with open(self.db_name, 'rb') as fp:
            while left > 0:
                chunk = fp.read(min(self.chunk, left))
                left -= len(chunk)
                yield chunk


class Application(object):

    def __init__(self, storage=None, dir=None,
                 include_syncclient=False,
                 secret_filename='/tmp/logdb-secret.txt'):
        if storage is None and dir:
            storage = UserStorage(dir)
        self.storage = storage
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

    def is_internal(self, req):
        ## FIXME: do actual authentication
        return True
        return req.environ.get('logdb.internal')

    def assert_is_internal(self, req):
        if not self.is_internal(req):
            raise exc.HTTPForbidden('authorized only for internal')

    def update_json(self, data, **kw):
        if isinstance(data, str):
            data = json.loads(data)
        data.update(kw)
        return data

    @wsgify
    def __call__(self, req):
        if self.include_syncclient and req.path_info == '/syncclient.js':
            return self.syncclient(req)
        script_name, path_info = req.script_name, req.path_info
        if path_info == '/verify':
            return self.verify(req)
        if path_info == '/node-added':
            return self.node_added(req)
        if path_info == '/remove-self':
            return self.remove_self(req)
        if path_info == '/query-deprecate':
            return self.query_deprecate(req)
        self.annotate_auth(req)
        domain = req.path_info_pop()
        username = req.path_info_pop()
        bucket = req.path_info
        req.script_name, req.path_info = script_name, path_info
        if domain is None or username is None or not bucket:
            return exc.HTTPNotFound('Not a valid URL: %r' % path_info)
        if not self.is_internal(req):
            resp = self._check_auth(req, username=username, domain=domain)
            if resp:
                return resp
        if 'include' in req.GET and 'exclude' in req.GET:
            raise exc.HTTPBadRequest('You may only include one of "exclude" or "include"')
        db = self.storage.for_user(domain, username, bucket)
        if 'copy' in req.GET:
            return self.copy(req, db)
        elif 'paste' in req.GET:
            return self.paste(req, db)
        elif 'deprecate' in req.GET:
            return self.deprecate(req, db)
        elif 'delete' in req.GET:
            return self.delete(req, db)
        elif 'backup-from-pos' in req.GET:
            return self.apply_backup(req, db)
        if db.is_deprecated:
            return Response(status=503, retry_after=60, body='Data in transit')
        if self.storage.is_disabled:
            return Response(status=503, retry_after=60, body='Server in process of retiring')
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
        if 'collection_id' not in req.GET and db.has_collection_id:
            resp_data = self.update_json(resp_data, collection_id=db.collection_id)
        if not isinstance(resp_data, str):
            resp_data = json.dumps(resp_data, separators=(',', ':'))
        resp = Response(resp_data, content_type='application/json')
        return resp

    def _check_auth(self, req, username, domain):
        remote_user = req.environ.get('REMOTE_USER')
        if not remote_user:
            return self.unauthorized('No authentication provided')
        if '/' not in remote_user:
            raise Exception(
                'REMOTE_USER is not formatted as username/domain')
        remote_username, remote_domain = remote_user.split('/', 1)
        if remote_domain.startswith('http'):
            remote_domain = urlparse.urlsplit(remote_domain).netloc.split(':')[0]
        if remote_username != username:
            return self.unauthorized('Incorrect authentication provided (%r != %r)' % (remote_username, username))
        if remote_domain != domain:
            return self.unauthorized('Incorrect authentication provided: bad domain (%r != %r)' % (remote_domain, domain))

    def post(self, req, db):
        try:
            data = req.json
        except ValueError:
            raise exc.HTTPBadRequest('POST must have valid JSON body')
        data_encoded = [json.dumps(i) for i in data]
        since = int(req.GET.get('since', 0))
        counter = None
        last_pos = db.db.length()
        try:
            counter = db.db.extend(data_encoded, expect_latest=since)
        except ExpectationFailed:
            pass
        if counter is None and 'include' in req.GET or 'exclude' in req.GET:
            failed = False
            for i in range(3):
                # Try up to three times to do this post, when there are soft failures.
                includes = req.GET.getall('include')
                excludes = req.GET.getall('exclude')
                for item_counter, item in db.db.read(since):
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
                    counter = db.db.extend(data_encoded, expect_latest=since)
                    break
                except ExpectationFailed:
                    pass
        if counter is None:
            resp_data = self.get(req, db)
            resp_data = self.update_json(resp_data, invalid_since=True)
            return resp_data
        counters = [counter + index for index in range(len(data))]
        if req.headers.get('X-Backup-To'):
            backups = [name.strip() for name in req.headers['X-Backup-To'].split(',')
                       if name.strip()]
            for backup in backups:
                self.post_backup(req, db, backup, last_pos)
        return dict(object_counters=counters)

    def post_backup(self, req, db, backup, last_pos):
        import urlparse
        from webob import Request
        from logdb.balancing.forwarder import forward
        url = urlparse.urljoin(req.application_url, '/' + backup)
        url += urllib.quote(req.path_info)
        if req.query_string:
            ## FIXME: not sure if 'since' should propagate, or maybe it doesn't matter?
            url += '?' + req.query_string
        backup_req = Request.blank(url, method='POST')
        backup_req.GET['backup-from-pos'] = str(last_pos)
        backup_req.GET['source'] = req.path_url
        backup_req.GET['collection_id'] = db.collection_id
        for key in 'exclude', 'include':
            if key in backup_req.GET:
                del backup_req.GET[key]
        backup_req.body = req.body
        backup_req.environ['logdb.root'] = req.environ.get('logdb.root')
        resp = forward(backup_req)
        #print 'sending backup req', backup_req, resp
        if resp.status_code >= 300:
            ## FIXME: what then?!
            print 'WARNING: bad response from %s: %s' % (backup_req.url, resp)

    def get(self, req, db):
        try:
            since = int(req.GET.get('since', 0))
        except ValueError:
            raise exc.HTTPBadRequest('Bad value since=%s' % req.GET['since'])
        try:
            limit = int(req.GET.get('limit', 0))
        except ValueError:
            raise exc.HTTPBadRequest('Bad value limit=%s' % req.GET['limit'])
        items = db.db.read(since)
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
        if auth:
            sig, data = auth.split('.', 1)
            if sign(get_secret(self._secret_filename), data) == sig:
                data = json.loads(data)
                req.environ['REMOTE_USER'] = data['email'] + '/' + data['audience']

    ## Internal/management methods

    def copy(self, req, db):
        self.assert_is_internal(req)
        if 'until' in req.GET:
            until = int(req.GET['until'])
        else:
            until = None
        encoded = db.encode_db(until)
        resp = Response(content_type='application/octet-stream',
                        app_iter=encoded,
                        content_length=encoded.length)
        return resp

    def paste(self, req, db):
        self.assert_is_internal(req)
        db.decode_db(req.body_file)
        return Response(status=201)

    def deprecate(self, req, db):
        self.assert_is_internal(req)
        if req.method != 'POST':
            return exc.HTTPMethodNotAllowed(allow='POST')
        db.deprecate()
        return Response(status=201)

    def delete(self, req, db):
        db.clear()
        return Response(status=201)

    def node_added(self, req):
        self.assert_is_internal(req)
        from logdb.balancing.forwarder import forward
        import urlparse
        from webob import Request
        from cStringIO import StringIO
        status = Response(content_type='text/plain')
        data = req.json
        dbs = []
        for other_node in data['other']:
            req_data = data.copy()
            req_data['name'] = other_node
            url = urlparse.urljoin(req.application_url, '/' + other_node)
            status.write('Deprecating from %s\n' % url)
            query = Request.blank(url + '/query-deprecate', json=req_data, method='POST')
            query.environ['logdb.root'] = req.environ.get('logdb.root')
            resp = forward(query)
            assert resp.status_code == 200, str(resp)
            resp_data = resp.json
            for db_data in resp_data['deprecated']:
                status.write('  deprecated: %(path)s\n' % db_data)
                dbs.append((other_node, db_data))
        for other_node, db_data in dbs:
            status.write('Copying database %s from %s\n' % (db_data['path'], other_node))
            url = urlparse.urljoin(req.application_url, '/' + other_node)
            copier = Request.blank(url + urllib.quote(db_data['path']) + '?copy')
            copier.environ['logdb.root'] = req.environ.get('logdb.root')
            resp = forward(copier)
            assert resp.status_code == 200, str(resp)
            ## FIXME: the terribleness!
            fp = StringIO(resp.body)
            db = self.storage.for_user(db_data['domain'], db_data['username'], db_data['bucket'])
            db.decode_db(fp)
            status.write('  copied %i bytes\n' % resp.content_length)
            deleter = Request.blank(url + db_data['path'] + '?delete')
            deleter.environ['logdb.root'] = req.environ.get('logdb.root')
            resp = forward(deleter)
            assert resp.status_code < 300, str(resp)
            status.write('  deleted\n')
        status.write('done.\n')
        return status

    def remove_self(self, req):
        self.assert_is_internal(req)
        from hash_ring import HashRing
        import urlparse
        from webob import Request
        from logdb.balancing.forwarder import forward
        status = Response(content_type='text/plain')
        self.storage.disable()
        data = req.json
        self_name = data['name']
        status.write('Disabling node %s\n' % self_name)
        ring = HashRing(data['other'])
        for domain, username, bucket in self.storage.all_dbs():
            assert bucket.startswith('/')
            path = '/' + domain + '/' + username + bucket
            db = self.storage.for_user(domain, username, bucket)
            if db.is_deprecated:
                db.clear()
                continue
            iterator = iter(ring.iterate_nodes(path))
            active_nodes = [iterator.next() for i in xrange(data['backups'] + 1)]
            new_node = iterator.next()
            assert self_name in active_nodes, '%r not in %r' % (self_name, active_nodes)
            status.write('Sending %s to node %s\n' % (path, new_node))
            url = urlparse.urljoin(req.application_url, '/' + new_node)
            send = Request.blank(url + urllib.quote(path) + '?paste',
                                 method='POST', body=''.join(db.encode_db()))
            send.environ['logdb.root'] = req.environ.get('logdb.root')
            resp = forward(send)
            assert resp.status_code == 201, str(resp)
            status.write('  success, deleting\n')
            db.clear()
        self.storage.clear()
        return status

    def query_deprecate(self, req):
        self.assert_is_internal(req)
        data = req.json
        nodes = data['other']
        self_name = data['name']
        new_node = data['new']
        backups = data['backups']
        from hash_ring import HashRing
        ring = HashRing(nodes + [new_node])
        deprecated = []
        for domain, username, bucket in self.storage.all_dbs():
            assert bucket.startswith('/')
            path = '/' + domain + '/' + username + bucket
            iterator = iter(ring.iterate_nodes(path))
            active_nodes = [iterator.next() for i in xrange(backups + 1)]
            deprecated_node = iterator.next()
            if deprecated_node == self_name and new_node in active_nodes:
                deprecated.append(
                    {'path': path, 'domain': domain, 'username': username, 'bucket': bucket})
                db = self.storage.for_user(domain, username, bucket)
                db.deprecate()
        return Response(json={'deprecated': deprecated})

    def apply_backup(self, req, db):
        from webob import Request
        from cStringIO import StringIO
        from logdb.balancing.forwarder import forward
        self.assert_is_internal(req)
        backup_pos = int(req.GET['backup-from-pos'])
        source = req.GET['source']
        collection_id = req.GET['collection_id']
        if collection_id != db.collection_id:
            if db.empty:
                db.set_collection_id(collection_id)
            else:
                dir, timer = db.dir, db.timer
                db.clear()
                db = Storage(dir, timer)
        items = req.json
        datas = [
            (backup_pos + index + 1, json.dumps(item))
            for index, item in enumerate(items)]
        try:
            db.db.extend(datas, expect_last_counter=backup_pos, with_counters=True)
        except ExpectationFailed:
            # The canonical server is ahead of us, we must catch up!
            has_queue = db.has_queue
            if has_queue:
                # We're in the middle of transferring, all is well
                db.queue.extend(datas, with_counters=True)
                ## FIXME: we should really try to extend the
            else:
                # We need to catch up
                catchup_req = Request.blank(source)
                catchup_req.GET['copy'] = ''
                catchup_req.GET['until'] = backup_pos
                catchup_req.environ['logdb.root'] = req.environ.get('logdb.root')
                resp = forward(catchup_req)
                assert resp.status_code == 200, str(resp)
                fp = StringIO(resp.body)
                db.decode_db(fp, append_queue=True)
        return Response(status=201)


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
