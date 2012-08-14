from webob.dec import wsgify
import urllib
import urlparse


@wsgify.middleware
def rooted(req, app):
    req.environ['logdb.root'] = (app, req.application_url)
    return app


def forward(req, new_req=None, root=None):
    if new_req is None:
        new_req = req
    if root is not None:
        return new_req.send(root)
    root = root_url = None
    if req.environ.get('logdb.root'):
        root, root_url = req.environ['logdb.root']
        new_req.environ['logdb.root'] = req.environ['logdb.root']
    if root_url and new_req.url.startswith(root_url):
        app_path = urlparse.urlsplit(root_url).path
        req_path = new_req.path
        assert req_path.startswith(app_path)
        new_req.path_info = urllib.unquote('/' + req_path[len(app_path):].lstrip('/'))
        new_req.script_name = urllib.unquote(app_path)
        return new_req.send(root)
    else:
        assert False, [root, root_url, new_req]
        return new_req.send()
