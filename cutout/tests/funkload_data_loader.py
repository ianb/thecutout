import sys
import os
import urllib
import random
import time
from cutout import Database


def load_users(dir, user_count, records, size, audience='example.com'):
    now = int(time.time())
    for i in xrange(user_count):
        user = 'user-%s@example.com' % i
        if isinstance(records, tuple):
            rec = random.randint(*records)
        else:
            rec = records
        fn = os.path.join(dir, urllib.quote(audience, ''), urllib.quote(user, '') + '.db')
        db = Database(fn)
        items = []
        for count in xrange(rec):
            if isinstance(size, tuple):
                s = random.randint(*size)
            else:
                s = size
            items.append('{"id": "item-%s-%s", "data": "%s"}' % (now, count, 'a'*s))
        db.extend(items)
        sys.stdout.write('.')
        sys.stdout.flush()
    print 'done.'

if __name__ == '__main__':
    import optparse
    parser = optparse.OptionParser(
        usage='%prog [OPTIONS] DIR')
    parser.add_option('--user-count', default=500, type='int', help="Number of users to load")
    parser.add_option('--records', default="5000-10000", help="Number or range of records to load")
    parser.add_option('--size', default="500-1000", help="Size of each record")
    options, args = parser.parse_args()
    if not args:
        parser.error("You must give a DIR")
    records = options.records
    if '-' in records:
        records = (int(records.split('-')[0]), int(records.split('-')[1]))
    else:
        records = int(records)
    size = options.size
    if '-' in size:
        size = (int(size.split('-')[0]), int(size.split('-')[1]))
    else:
        size = int(size)
    load_users(args[0], options.user_count, records, size)
