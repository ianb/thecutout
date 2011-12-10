import os
import re
import random
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
        if i == 8 + Database.INDEX_SIZE * 8:
            print '-' * 20
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
        print_data('beginning')
        result = db.extend(['1', '2', '3'])
        #print_data('3 items')
        self.assertEqual(result, 1)
        result = db.extend(['4', '5', '6'])
        #print_data('6 items')
        self.assertEqual(result, 4)
        for i in range(5000):
            last = db.extend(['x' * i] * 10)
        #print_data('a bunch of stuff')
        for i in range(5000):
            pos = random.randint(1, last)
            length = random.randint(1, 100)
            db.read(pos, length)
        self.assertEqual(list(db.read(1, 2)), [(1, '1'), (2, '2')])
        self.assertEqual(list(db.read(1, 3)), [(1, '1'), (2, '2'), (3, '3')])
        self.assertEqual(list(db.read(3, 4)), [(3, '3'), (4, '4'), (5, '5'), (6, '6')])
