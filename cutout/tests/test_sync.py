import os
import shutil
import urllib
import webtest
import simplejson as json
from itertools import count
from unittest2 import TestCase
from cutout.sync import Application, UserStorage

here = os.path.dirname(os.path.abspath(__file__))
test_dir = os.path.join(here, 'test-sync-dbs')

with open(os.path.join(here, 'fake-assertion.txt')) as fp:
    fake_assertion = fp.read().strip()


class TestSync(TestCase):

    def setUp(self):
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)
        os.makedirs(test_dir)
        self.wsgi_app = Application(UserStorage(test_dir, timer=count().next))
        self.app = webtest.TestApp(self.wsgi_app)
        self.auth = None

    def url(self, domain='example.com', username='test@example.com', **kw):
        url = '/%s/%s/bucket' % (urllib.quote(domain), urllib.quote(username))
        if kw:
            url += '?' + urllib.urlencode(kw)
        return url

    def setup_auth(self):
        if self.auth is None:
            self.auth = 'BrowserID %s' % fake_assertion

    def get(self, **kw):
        self.setup_auth()
        resp = self.app.get(self.url(**kw),
                            headers=dict(Authorization=self.auth))
        new_auth = resp.headers.get('X-Set-Authorization')
        if new_auth:
            self.auth = new_auth
        return resp

    def post(self, body, **kw):
        self.setup_auth()
        resp = self.app.post(self.url(**kw), json.dumps(body),
                             headers=dict(Authorization=self.auth))
        new_auth = resp.headers.get('X-Set-Authorization')
        if new_auth:
            self.auth = new_auth
        return resp

    def test_simple(self):
        resp = self.get()
        self.assertEqual(resp.json, dict(objects=[]))
        resp = self.post([dict(id="test", data="whatever"), dict(id="test2", data="whatever")])
        self.assertEqual(resp.json, dict(object_counters=[1, 2]))
        resp = self.get(since=1)
        self.assertEqual(resp.json, dict(objects=[[2, dict(data='whatever', id='test2')]]))
        resp = self.get(since=2)
        self.assertEqual(resp.json, dict(objects=[]))
