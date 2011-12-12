import time
import os
import urllib
from appsync.storage import (IAppSyncDatabase, CollectionDeletedError, StorageAuthError)
from mozsvc.util import maybe_resolve_name
import vep
import simplejson as json
from logdb import Database
from zope.interface import implements


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

    def _raise_deleted(self, user, collection):
        """Raises CollectionDeletedError if appropriate"""
        fp = open(self.filename(user, collection, '.metadata'), 'rb')
        metadata = json.load(fp)
        fp.close()
        if metadata.get('deleted'):
            raise CollectionDeletedError(metadata['client_id'], metadata['reason'])

    def get_db(self, user, collection):
        return Database(self.filename(user, collection, '.db'))

    def filename(self, user, collection, type):
        return os.path.join(self.dir, urllib.quote(user, '') + '-' + urllib.quote(collection, '') + type)

    def delete(self, user, collection, client_id, reason, token):
        self.get_db(user, collection).delete()
        fp = open(self.filename(user, collection, '.metadata'), 'wb')
        fp.write(json.dumps(dict(deleted=True, client_id=client_id, reason=reason)))

    def get_uuid(self, user, collection, token):
        fp = open(self.filename(user, collection, '.metadata'), 'rb')
        data = json.loads(fp.read())
        fp.close()
        return data.get('uuid')

    def get_applications(self, user, collection, since, token):
        self._raise_deleted(user, collection)
        db = self.get_db(user, collection)
        assert isinstance(since, int)
        for count, item in db.read(since):
            item = json.loads(item)
            yield count, item['data']

    def _make_new_metadata(self, user, collection):
        uuid = '%s-%s' % (time.time(), collection)
        fp = open(self.filename(user, collection, '.metadata'), 'wb')
        fp.write(json.dumps(dict(uuid=uuid)))
        fp.close()

    def add_applications(self, user, collection, applications, token):
        try:
            fp = open(self.filename(user, collection, '.metadata'), 'rb')
        except IOError:
            self.make_new_metadata(user, collection)
        else:
            metadata = json.load(fp)
            fp.close()
            if metadata['deleted']:
                fp = open(self.filename(user, collection, '.metadata'), 'wb')
                self.make_new_metadata(user, collection)
        db = self.get_db(user, collection)
        datas = []
        for app in applications:
            datas.append({'id': app['origin'], 'type': 'app', 'data': app})
        db.extend(datas)

    def get_last_modified(self, user, collection):
        db = self.get_db(user, collection)
        return db.length()

    def verify(self, assertion, audience):
        try:
            email = self._verifier.verify(assertion, audience)["email"]
        except (ValueError, vep.TrustError):
            return 'whatever', 'xxx'
            raise StorageAuthError
        token = 'CREATE A TOKEN HERE XXX'
        return email, token
