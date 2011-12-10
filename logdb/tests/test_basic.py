import os
import re
import random
import time
from logdb import Database
from unittest2 import TestCase

tmp_filename = '/tmp/test.db'


def create_db():
    if os.path.exists(tmp_filename):
        os.unlink(tmp_filename)
    #Database.INDEX_SIZE = 4
    db = Database(tmp_filename)
    return db


def print_data(header=None):
    if header:
        print '== %s' % header
    fp = open(tmp_filename, 'rb')
    data = fp.read()
    for i in range(0, len(data), 4):
        print '%3i' % i,
        for j in range(4):
            if len(data) > i + j:
                print '%02x' % ord(data[i + j]),
        chunk = data[i:i + 4]
        chunk = re.sub(r'[^\x20-\x80]', ' ', chunk)
        print chunk


class TestBasic(TestCase):
    def test_operations(self):
        db = create_db()
        #print_data('beginning')
        start = time.time()
        result = db.extend(['1', '2', '3'])
        #print_data('3 items')
        self.assertEqual(result, 1)
        result = db.extend(['4', '5', '6'])
        #print_data('6 items')
        self.assertEqual(result, 4)
        for i in range(1000):
            last = db.extend(['x' * i] * 10)
        end_write = time.time()
        print 'Time to write %s items: %i seconds (%i/second)' % (
            db.length(), end_write - start, db.length() / (end_write - start))
        self.assertEqual(list(db.read(last + 8)), [(10006, 'x' * 999)])
        self.assertEqual(list(db.read(last + 9)), [])
        all = list(db.read(0))
        self.assertEqual(list(reversed(all[-6:])), [(1, '1'), (2, '2'), (3, '3'), (4, '4'), (5, '5'), (6, '6')])
        READ_COUNT = 100
        for i in xrange(READ_COUNT):
            pos = random.randint(1, last)
            list(db.read(pos))
        end_read = time.time()
        print 'Time to read %s items anywhere: %i seconds (%i/second)' % (
            READ_COUNT, end_read - end_write, READ_COUNT / (end_read - end_write))
        SMALL_COUNT = 1000
        for i in xrange(SMALL_COUNT):
            pos = random.randint(last - 100, last)
            list(db.read(pos))
        end_small = time.time()
        print 'Time to read %s small items (from last 1-100 items): %i seconds (%i/second)' % (
            SMALL_COUNT, end_small - end_read, SMALL_COUNT / (end_small - end_read))



if __name__ == '__main__':
    import cProfile
    cProfile.run('TestBasic("test_operations").test_operations()')
