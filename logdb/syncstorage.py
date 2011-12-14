import time
import os
import urllib
import simplejson as json
import vep
from zope.interface import implements
from mozsvc.util import maybe_resolve_name
from appsync.storage import (IAppSyncDatabase, CollectionDeletedError, StorageAuthError)
from appsync.util import gen_uuid
from appsync.cache import Cache   # XXX should use it via plugin conf.
from logdb import Database


class SyncStorage(object):
    implements(IAppSyncDatabase)

    def __init__(self, **options):
        verifier = options.pop("verifier", None)
        if verifier is None:
            verifier = vep.RemoteVerifier()
        else:
            verifier = maybe_resolve_name(verifier)
            if callable(verifier):
                verifier = verifier()
        self._verifier = verifier
        self.dir = options['dir']
        if not os.path.exists(self.dir):
            os.makedirs(self.dir)
        self.session_ttl = int(options.get('session_ttl', '300'))
        cache_options = {'servers': options.get('cache_servers', '127.0.0.1'),
                         'prefix': options.get('cache_prefix', 'appsyncsql')}

        self.cache = Cache(**cache_options)
        self.authentication = True

    def set_authentication(self, state):
        self.authentication = state

    def _raise_deleted(self, user, collection):
        """Raises CollectionDeletedError if appropriate"""
        filename = self.filename(user, collection, '.deleted')
        if os.path.exists(filename):
            fp = open(filename, 'rb')
            metadata = json.load(fp)
            fp.close()
            raise CollectionDeletedError(metadata['client_id'], metadata['reason'])

    def get_db(self, user, collection):
        return Database(self.filename(user, collection, '.db'))

    def filename(self, user, collection, type):
        return os.path.join(self.dir, urllib.quote(user, '') + '-' + urllib.quote(collection, '') + type)

    def delete(self, user, collection, client_id, reason, token):
        self._check_token(token)
        ## FIXME: file locking?
        self.get_db(user, collection).delete()
        uuid_name = self.filename(user, collection, '.uuid')
        if os.path.exists(uuid_name):
            os.unlink(uuid_name)
        fp = open(self.filename(user, collection, '.deleted'), 'wb')
        fp.write(json.dumps(dict(deleted=True, client_id=client_id, reason=reason)))
        fp.close()

    def get_uuid(self, user, collection, token):
        self._check_token(token)
        filename = self.filename(user, collection, '.uuid')
        if not os.path.exists(filename):
            return None
        else:
            fp = open(filename, 'rb')
            try:
                return fp.read().strip()
            finally:
                fp.close()

    def get_applications(self, user, collection, since, token):
        self._check_token(token)
        self._raise_deleted(user, collection)
        db = self.get_db(user, collection)
        since = int(since)
        return [(count, json.loads(item)) for count, item in db.read(since)]

    def _make_new_uuid(self, user, collection):
        uuid = '%s-%s' % (time.time(), collection)
        fp = open(self.filename(user, collection, '.uuid'), 'wb')
        fp.write(uuid)
        fp.close()
        return uuid

    def add_applications(self, user, collection, applications, token):
        self._check_token(token)
        del_filename = self.filename(user, collection, '.deleted')
        if os.path.exists(del_filename):
            os.unlink(del_filename)
        uuid_filename = self.filename(user, collection, '.uuid')
        if not os.path.exists(uuid_filename):
            self._make_new_uuid(user, collection)
        db = self.get_db(user, collection)
        datas = []
        for app in applications:
            datas.append(json.dumps({'id': app['origin'], 'type': 'app', 'data': app}))
        db.extend(datas)

    def get_last_modified(self, user, collection, token):
        self._check_token(token)
        db = self.get_db(user, collection)
        return db.length()

    def verify(self, assertion, audience):
        """Authenticate then return a token"""
        if not self.authentication:
            raise NotImplementedError('authentication not actrivated')

        try:
            email = self._verifier.verify(assertion, audience)["email"]
        except (ValueError, vep.TrustError), e:
            raise StorageAuthError(e.message)

        # create the token and create a session with it
        token = gen_uuid(email, audience)
        self.cache.set(token, (email, audience), time=self.session_ttl)
        return email, token

    def _check_token(self, token):
        if not self.authentication:
            # bypass authentication
            return

        # XXX do we want to check that the user owns that path ?
        res = self.cache.get(token)
        if res is None:
            raise StorageAuthError()
