import os
import unittest
import urllib
import random
import simplejson as json
from funkload.FunkLoadTestCase import FunkLoadTestCase
from funkload.utils import Data

here = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(here, 'fake-assertion.txt')) as fp:
    fake_assertion = fp.read().strip()


class Simple(FunkLoadTestCase):

    def setUp(self):
        self.server_url = self.conf_get('main', 'url')
        self.user_count = int(self.conf_get('main', 'user_count'))
        self.user = 'user-%s@example.com' % random.randint(0, self.user_count)
        self.authorization = 'BrowserID %s' % fake_assertion
        self.setHeader('Authorization', self.authorization)
        self.since = 0
        self.biggest_since = 0
        self.collection_id = None

    def url(self, domain='example.com', username=None, **kw):
        username = username or self.user
        if self.collection_id:
            kw['collection_id'] = self.collection_id
        url = '%s/%s/%s' % (self.server_url, urllib.quote(domain), urllib.quote(username))
        if kw:
            url += '?' + urllib.urlencode(kw)
        return url

    def get_bunch(self, limit=50):
        while 1:
            resp = self.get(self.url(since=self.since, limit=limit), description='Get data')
            if resp.headers.get('x-set-authorization'):
                self.authorization = resp.headers['x-set-authorization']
                self.setHeader('Authorization', self.authorization)
            self.assertEqual(resp.code, 200)
            data = json.loads(resp.body)
            if 'collection_id' in data:
                self.collection_id = data['collection_id']
            if data['objects']:
                self.since = data['objects'][-1][0]
                self.biggest_since = max(self.since, self.biggest_since)
            if not data.get('incomplete'):
                break

    def post_bunch(self, items=20, size=500):
        items = [
            dict(id="item-%s" % random.random(),
                 data="x" * size)
            for i in range(items)]
        while 1:
            resp = self.post(self.url(since=self.since), params=Data('application/json', json.dumps(items)),
                             description="Adding data")
            if resp.headers.get('x-set-authorization'):
                self.authorization = resp.headers['x-set-authorization']
                self.setHeader('Authorization', self.authorization)
            self.assertEqual(resp.code, 200)
            data = json.loads(resp.body)
            if data.get('since_invalid'):
                self.since = data['objects'][-1][0]
                self.biggest_since = max(self.since, self.biggest_since)
                continue
            break

    def test_simple(self, loops=2):
        # we'll do a post, then 10 gets, then a post, and so on
        for i in xrange(loops):
            self.post_bunch()
            for j in xrange(10):
                if self.biggest_since and not j % 3:
                    self.since = random.randint(1, self.biggest_since)
                self.get_bunch()


if __name__ in ('main', '__main__'):
    unittest.main()
