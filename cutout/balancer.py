"""WSGI application that distributes sync requests to various nodes.
"""
import os
from webob.dec import wsgify
from webob import Request
from hash_ring import HashRing
import urllib
import urlparse
from cutout import sync
from cutout.forwarder import forward


class Application(object):
    """Application to route requests to nodes, and backup nodes"""

    def __init__(self, preload=None, preload_dir=None, backups=1):
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
        self.backups = backups

    @wsgify
    def __call__(self, req):
        first = req.path_info_peek()
        if first in self.subnodes:
            req.path_info_pop()
            return self.subnodes[first]
        path = req.path_info
        iterator = iter(self.ring.iterate_nodes(path))
        subnode_url = iterator.next()
        subnode = SubNode(subnode_url)
        if self.backups and req.method == 'POST':
            backup_to = []
            for i in xrange(self.backups):
                backup_to.append(iterator.next())
            req.headers['X-Backup-To'] = ', '.join(backup_to)
        return req.send(subnode)

    def add_node(self, url, create=False, root=None):
        """Adds a new node, with the given url/name"""
        if create:
            dir = os.path.join(self.basedir, url)
            app = sync.Application(dir=dir)
            self.subnodes[url] = app
        node = SubNode(url)
        node.added(self.ring.nodes, backups=self.backups, root=root)
        self.ring = HashRing(self.ring.nodes + [url])

    def remove_node(self, url, root=None, force=False):
        """Removes the given node (according to its url/name)

        If force=True then the node is removed without its cooperation
        """
        node = SubNode(url)
        node.remove(self.ring.nodes, backups=self.backups, force=force, root=root)
        new_nodes = list(self.ring.nodes)
        new_nodes.remove(url)
        self.ring = HashRing(new_nodes)

    def node_list(self, url):
        """Returns a list of the master node and backup nodes for the
        given request URL"""
        iterator = iter(self.ring.iterate_nodes(url))
        nodes = [iterator.next() for i in xrange(self.backups + 1)]
        return nodes


class SubNode(object):
    """Represents one node."""

    def __init__(self, url):
        self.url = url

    @wsgify
    def __call__(self, req):
        """Forwards a request to the node"""
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

    def added(self, other_nodes, backups=0, root=None):
        """Called when the node was added"""
        req = Request.blank(
            self.url + '/node-added', json={'other': other_nodes, 'new': self.url, 'backups': backups})
        print forward(req, root=root).body.strip()

    def remove(self, other_nodes, backups=0, root=None, force=False):
        """Called when the node was removed"""
        if force:
            for other in other_nodes:
                if other == self.url:
                    continue
                node = SubNode(other)
                node.take_over(self.url, other_nodes, backups=backups, root=root)
        else:
            req = Request.blank(
                self.url + '/remove-self', json={'other': other_nodes, 'name': self.url, 'backups': backups})
            print forward(req, root=root).body.strip()

    def take_over(self, bad_node, other_nodes, backups=0, root=None):
        """Called when the node should take over from `bad_node`"""
        req = Request.blank(
            self.url + '/take-over', json={'other': other_nodes, 'bad': bad_node, 'name': self.url, 'backups': backups})
        body = forward(req, root=root).body.strip()
        if body:
            print body
