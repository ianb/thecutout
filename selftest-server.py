#!/usr/bin/env python
"""
Test server, for running the cutout/sync tests
"""
import optparse
import sys
import os
import shutil
import site

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, here)

path = here
checks = ['vendor', 'vendor-binary']
for i in range(3):
    for check in checks:
        dir = os.path.join(path, check)
        if os.path.exists(dir):
            old_sys_path = list(sys.path)
            site.addsitedir(dir)
            for new_path in list(sys.path):
                if new_path not in old_sys_path:
                    sys.path.remove(new_path)
                    sys.path.insert(0, new_path)
    path = os.path.dirname(path)

parser = optparse.OptionParser(
    usage='%prog [OPTIONS]')

parser.add_option('-H', '--host', metavar='HOST', default='localhost')
parser.add_option('-p', '--port', metavar='PORT', default='8088')
parser.add_option('--dir', metavar='DIRECTORY', default='./data')
parser.add_option('--clear', action='store_true')


from webob.dec import wsgify


@wsgify.middleware
def TestAuthMiddleware(req, app):
    req.environ['REMOTE_USER'] = req.headers.get('X-Remote-User')
    return app


def main():
    options, args = parser.parse_args()
    if options.clear:
        if os.path.exists(options.dir):
            print 'Deleting %s' % options.dir
            shutil.rmtree(options.dir)
    from cutout.sync import Application
    db_app = Application(dir=options.dir, include_syncclient=True)
    from webob.static import DirectoryApp
    from paste.urlmap import URLMap
    map = URLMap()
    map['/'] = DirectoryApp(
        os.path.join(here, 'cutout', 'tests', 'syncclient'))
    map['/db'] = TestAuthMiddleware(db_app)
    from paste.httpserver import serve
    serve(map, host=options.host, port=int(options.port))


if __name__ == '__main__':
    main()
