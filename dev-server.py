#!/usr/bin/env python
"""
Dev server, for running your own apps (while testing/developing)
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
    usage='%prog [OPTIONS] [[path=]STATIC_DIR]',
    description="""\
Serves up logdb at http://localhost:8088/sync/syncclient.js

Also serves up static files.  You may provide one or many STATIC_DIR
entries.  You may give prefixes for these (e.g.,
/test1=~/src/myproject/htdocs)
""")

parser.add_option('-H', '--host', metavar='HOST', default='localhost',
                  help='Host (interface) to serve on; 0.0.0.0 means serve publicly')
parser.add_option('-p', '--port', metavar='PORT', default='8088',
                  help='Port to serve on')
parser.add_option('--dir', metavar='DIRECTORY', default='./data',
                  help='Directory to store files in')
parser.add_option('--clear', action='store_true',
                  help='Clear DIRECTORY on startup')

from paste.urlmap import URLMap
from paste.httpserver import serve
from webob.static import DirectoryApp


def main():
    options, args = parser.parse_args()
    if options.clear:
        if os.path.exists(options.dir):
            print 'Deleting %s' % options.dir
            shutil.rmtree(options.dir)
    mapper = URLMap()
    for arg in args:
        if '=' in arg:
            path, dir = arg.split('=', 1)
        else:
            path, dir = '/', arg
        mapper[path] = DirectoryApp(dir)
    from logdb.sync import Application
    db_app = Application(dir=options.dir, include_syncclient=True)
    mapper['/sync'] = db_app
    serve(mapper, host=options.host, port=int(options.port))


if __name__ == '__main__':
    main()
