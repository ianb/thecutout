"""Implements the sync server

This does not include routing and balancing, but does include the
actual database handling."""

import os
import shutil
import time
import urllib
import urlparse
from cStringIO import StringIO
try:
    import simplejson as json
except ImportError:
    import json
from webob.dec import wsgify
from webob import Response, Request
from webob import exc
from hash_ring import HashRing
from logdb import Database, ExpectationFailed
from logdb import int_encoding
from logdb.forwarder import forward


syncclient_filename = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    'syncclient.js')


class StorageDeprecated(Exception):
    """Raised when you try to access a database that has been deprecated"""


class UserStorage(object):
    """A container for multiple databases."""

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
    """A single database."""

    def __init__(self, dir, timer=time.time):
        self.dir = dir
        if not os.path.exists(dir):
            os.makedirs(dir)
        self.timer = timer
        self._collection_id = None

    @property
    def collection_id(self):
        """Reads the collection_id from disk, creating if necessary"""
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
        """Indicates if a collection_id has already been set on this
        database.  ``GET`` requests to non-existant databases try to
        avoid prematurely setting collection_id."""
        col_filename = os.path.join(self.dir, 'collection_id.txt')
        return os.path.exists(col_filename)

    def clear(self):
        """Clears this database entirely."""
        shutil.rmtree(self.dir)

    @property
    def is_deprecated(self):
        """A database can be deprecated, with the data still around
        but not active.  Then ``.deprecated_db`` will work, but
        ``.db`` will not"""
        return os.path.exists(os.path.join(self.dir, 'deprecated'))

    @property
    def db(self):
        """Returns the logdb database"""
        if self.is_deprecated:
            raise StorageDeprecated()
        db_name = os.path.join(self.dir, 'database')
        db = Database(db_name)
        return db

    @property
    def deprecated_db(self):
        """Returns the logdb dataabse, if this is deprecated"""
        db_name = os.path.join(self.dir, 'deprecated')
        if not os.path.exists(db_name):
            raise IOError("File does not exist: %r" % db_name)
        db = Database(db_name)
        return db

    @property
    def has_queue(self):
        """Indicates if this database has a pending queue (objects
        that should be appended to the database, but have not yet
        been)"""
        return os.path.exists(os.path.join(self.dir, 'queue'))

    @property
    def queue_db(self):
        """The queue logdb database"""
        db_name = os.path.join(self.dir, 'queue')
        db = Database(db_name)
        return db

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
        """Deprecates the database"""
        if self.is_deprecated:
            return
        db_name = os.path.join(self.dir, 'database')
        os.rename(db_name, os.path.join(self.dir, 'deprecated'))
        os.rename(db_name + '.index', os.path.join(self.dir, 'deprecated.index'))

    def encode_db(self, until=None):
        """Returns an iterator that yields the encoded database, for
        use with ``?copy/?paste``"""
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
        """Decodes the encoded database, as found in the file-like
        `fp` object.  Overwrites colletion_id and the database"""
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
        """Updates the JSON data `data` with any given keyword keys.
        The data may be a dictionary or an encoded JSON object.
        """
        if isinstance(data, str):
            data = json.loads(data)
        data.update(kw)
        return data

    @wsgify
    def __call__(self, req):
        """Responds to and routes all requests
        """
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
        if path_info == '/take-over':
            return self.take_over(req)
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
        """Responds to ``POST /db-name``

        Handles the public interface for adding to a database.
        """
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
        """Handles backups from a POST request.

        Forwards the request, as come from the given database, and
        with the known last position of this database.  `backup` is
        the node to back up to.
        """
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
        """Responds to ``GET /db-name``

        Returns the (public-interface) GET request.
        """
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
        """Handles ``GET /db-name?include=...|exclude=...``

        Filters out some items.
        """
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
        """Responds to ``GET /syncclient.js``

        Returns syncclient.js, with a substitution to point it to this
        server.
        """
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
        """Responds to ``POST /verify``

        This checks a BrowserID/Persona assertion, and returns
        information on how to authenticate future requests.
        """
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
        """Adds ``REMOTE_USER`` to ``req.environ``

        Checks for a ``?auth=sig`` to set user.  If REMOTE_USER is
        already set then this doesn't undo that.
        """
        auth = req.GET.get('auth')
        if auth:
            sig, data = auth.split('.', 1)
            if sign(get_secret(self._secret_filename), data) == sig:
                data = json.loads(data)
                req.environ['REMOTE_USER'] = data['email'] + '/' + data['audience']

    def delete(self, req, db):
        """Responds to ``/db-name?delete`` - deletes a database
        """
        db.clear()
        return Response(status=201)

    ## Internal/management methods

    def copy(self, req, db):
        """Responds to ``GET /db-name?copy`` - get a dump of the entire database.

        This returns an binary encoded version of the entire database,
        optionally up until ``?until=count`` (if omitted, then the
        entire database).  This includes the collection_id.
        """
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
        """Responds to ``POST /db-name?paste`` - overwrite the entire database.

        Accepts an encoded database in the body.
        """
        self.assert_is_internal(req)
        db.decode_db(req.body_file)
        return Response(status=201)

    def deprecate(self, req, db):
        """Responds to ``POST /db-name?deprecate`` - deprecates a single database

        A deprecated database isn't dead, but is filed away and can't
        be added to.
        """
        self.assert_is_internal(req)
        if req.method != 'POST':
            return exc.HTTPMethodNotAllowed(allow='POST')
        db.deprecate()
        return Response(status=201)

    def node_added(self, req):
        """Responds to ``POST /node-added``

        This is called to ask this node to take over from any other
        nodes, as appropriate.

        Takes a request with the JSON body:

        `other`: list of all nodes.
        `name`: the name of this node.

        Responds with a text description of what it did.
        """
        self.assert_is_internal(req)
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
        """Responds to ``POST /remove-self``

        This is a request for this node to gracefully remove itself.
        It will attempt to back up its data to the other nodes that
        should take over.

        This takes a request with the JSON data:

        `name`: the name of this node
        `other`: a list of all nodes (including this)
        `backups`: the number of backups to make

        It responds with a text description of what it did.
        """
        self.assert_is_internal(req)
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
        """Responds to ``POST /query-deprecate``

        This is used when a new node is added to the system, and all
        existing nodes are asked for what databases should be assigned
        to the new node.  Any such database will be deprecated, and a
        list of those databases is returned.

        Accepts a JSON body with the keys:

        `other`: list of all nodes
        `name`: the name of this node
        `new`: the node being added
        `backups`: the number of backups to keep

        Returns JSON::

            {"deprecated": [deprecated items]}

        Where the deprecated items are::

            {"path": "/domain/user/bucket",
             "domain": "domain",
             "username": "user",
             "bucket": "bucket"
            }
        """
        self.assert_is_internal(req)
        data = req.json
        nodes = data['other']
        self_name = data['name']
        new_node = data['new']
        backups = data['backups']
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
        """Responds to ``POST /db-name?backup-from-pos=N``

        This is the request that is sent when the master node wants to
        backup a POST request to this backup node.  This is handled
        similar to a POST request, but the data is kept
        unconditionally.  When a backup arrives but is ahead of local
        records, this node will try to catch up with a `?copy` request.

        This has the additional GET parameters of:

        `backup-from-pos`: what the last id was on the master node; if
        it's ahead of what we have then we need to catch up.

        `source`: the master node.
        """
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

    def take_over(self, req):
        """Attached to ``POST /take-over``

        Takes over databases from another server, that presumably has
        gone offline without notice.

        This goes through all of the local databases, and sees if this
        node was either using the bad node as a backup, or is a backup
        for the bad node.  In either case it finds the new node that
        should be either master or handling the bad node, and sends
        the local database to that server.

        Takes a JSON body with keys:

        `other`: a list of all nodes
        `name`: the name of *this* node
        `bad`: the bad node being removed
        `backups`: the number of backups
        """
        self.assert_is_internal(req)
        status = Response(content_type='text/plain')
        data = req.json
        nodes = data['other']
        self_name = data['name']
        bad_node = data['bad']
        assert self_name != bad_node
        backups = data['backups']
        ring = HashRing(nodes)
        for domain, username, bucket in self.storage.all_dbs():
            assert bucket.startswith('/')
            path = '/' + domain + '/' + username + bucket
            iterator = iter(ring.iterate_nodes(path))
            active_nodes = [iterator.next() for i in xrange(backups + 1)]
            replacement_node = iterator.next()
            # Not all the backups should try to restore the database, so instead
            # just the "first" does it
            restore = False
            if active_nodes[0] == bad_node and active_nodes[1:] and active_nodes[1] == self_name:
                status.write('Master node %s for %s removed\n' % (bad_node, path))
                restore = True
            elif bad_node in active_nodes and active_nodes[0] == self_name:
                status.write('Backup node %s for %s removed\n' % (bad_node, path))
                restore = True
            if not restore:
                continue
            db = self.storage.for_user(domain, username, bucket)
            send = Request.blank(replacement_node + urllib.quote(path) + '?paste',
                                 method='POST', body=''.join(db.encode_db()))
            send.environ['logdb.root'] = req.environ.get('logdb.root')
            resp = forward(send)
            assert resp.status_code == 201, str(resp)
            #status.write('  nodes: %r - %r / %r\n' % (active_nodes, bad_node, self_name))
            status.write('  success, added to %s (from %s)\n' % (replacement_node, self_name))
        return status


def b64_encode(s):
    """Compact/url-safe base64 encoding"""
    import base64
    return base64.urlsafe_b64encode(s).strip('=').strip()


def get_secret(filename):
    """Retrieves the secret from a filename, generating a secret if necessary."""
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
    """Sign the text using the secret"""
    import hmac
    import hashlib
    return b64_encode(hmac.new(secret, text, hashlib.sha1).digest())
