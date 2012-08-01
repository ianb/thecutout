import os
from webob.dec import wsgify
from webob import Request
from hash_ring import HashRing
import urllib
import urlparse
from logdb import sync
from logdb.balancing.forwarder import forward


class Application(object):

    def __init__(self, preload=None, preload_dir=None):
        self.subnodes = {}
        self.basedir = preload_dir
        nodes = []
        if preload:
            for i in xrange(preload):
                name = 'node-%03i' % i
                dir = os.path.join(preload_dir, name)
                app = sync.Application(dir=dir)
                self.subnodes[name] = app
                nodes.append(name)
        self.ring = HashRing(nodes)

    @wsgify
    def __call__(self, req):
        first = req.path_info_peek()
        if first in self.subnodes:
            req.path_info_pop()
            return self.subnodes[first]
        path = req.path_info
        subnode = self.select_node(path)
        return req.send(subnode)

    def add_node(self, url, create=False, root=None):
        if create:
            dir = os.path.join(self.basedir, url)
            app = sync.Application(dir=dir)
            self.subnodes[url] = app
        node = SubNode(url)
        node.added(self.ring.nodes, root)
        self.ring = HashRing(self.ring.nodes + [url])

    def select_node(self, path):
        node_url = self.ring.get_node(path)
        return SubNode(node_url)


class SubNode(object):

    def __init__(self, url):
        self.url = url

    @wsgify
    def __call__(self, req):
        url = urlparse.urljoin(req.application_url, self.url)
        parsed = urlparse.urlsplit(url)
        req.scheme = parsed.scheme
        req.host = parsed.netloc
        req.server_name = parsed.hostname
        req.server_port = parsed.port or '80'
        req.script_name = urllib.unquote(parsed.path)
        resp = forward(req)
        resp.headers['X-Node-Name'] = self.url
        return resp

    def added(self, other_nodes, root):
        req = Request.blank(
            self.url + '/node-added', json={'other': other_nodes, 'new': self.url})
        print forward(req, root=root)
