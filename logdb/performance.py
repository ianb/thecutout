import random
import string
import os
import sys
import time
from logdb import Database
from logdb.streamdb import Database as StreamDatabase
from logdb.sql import MySQLStorage

DATA = string.ascii_letters


class Counter(object):

    def __init__(self):
        self.types = {}
        self.pending = {}

    def start(self, name):
        self.pending[name] = time.time()

    def end(self, name, count):
        total = time.time() - self.pending.pop(name)
        existing = self.types.get(name, (0, 0))
        self.types[name] = (existing[0] + count, existing[1] + total)

    def summarize(self):
        assert not self.pending
        for name in sorted(self.types):
            count, total = self.types[name]
            print '%i %s in %.1f seconds (%i/second)' % (
                count, name, total, (count / total))


class Runner(object):

    def __init__(self, db_count, read_portion, large_read_portion,
                 db_loader):
        self.dbs = [
            'db%i' % num for num in range(db_count)]
        self.read_portion = read_portion
        self.large_read_portion = large_read_portion
        self.db_loader = db_loader
        self.counter = Counter()
        for db_name in self.dbs:
            db = self.db_loader(db_name)
            db.clear()

    def preload(self, count):
        self.counter.start('preload')
        for db_name in self.dbs:
            db = self.db_loader(db_name)
            db.extend([DATA] * count)
        self.counter.end('preload', count * len(self.dbs))

    def run(self, times=1):
        db_name = random.choice(self.dbs)
        db = self.db_loader(db_name)
        if random.random() < self.read_portion:
            if random.random() < self.large_read_portion:
                self.run_large_reads(db, times)
            else:
                self.run_reads(db, times)
        else:
            self.run_writes(db, times)

    def run_reads(self, db, times):
        length = db.length()
        if not length:
            self.run_writes(db, times)
            return
        self.counter.start('reads')
        for i in xrange(times):
            lowest = max(1, length - 100)
            start = random.randint(lowest, length)
            list(db.read(start))
        self.counter.end('reads', times)

    def run_large_reads(self, db, times):
        length = db.length()
        self.counter.start('large reads')
        for i in xrange(times):
            start = random.randint(1, length)
            list(db.read(start))
        self.counter.end('large reads', times)

    def run_writes(self, db, times):
        ## Generally we don't write big chunks, even if we write
        ## somewhat frequently
        times = max(1, times / 5)
        self.counter.start('writes')
        for i in xrange(times):
            db.extend([DATA] * 5)
        self.counter.end('writes', times)

    def run_many(self, reps, times):
        self.counter.start('total')
        for i in xrange(reps):
            if i and not i % 25:
                sys.stdout.write('\r%i/%i  %3i%%    ' % (i, reps, 100 * i / reps))
                sys.stdout.flush()
            self.run(times)
        self.counter.end('total', 1)
        sys.stdout.write('\r                                \r')
        sys.stdout.flush()


import optparse

parser = optparse.OptionParser(
    usage='%prog [-m MYSQL_OPTION=VALUE DBNAME] or [DIR_NAME]')

parser.add_option(
    '-m', '--mysql',
    help='Use MySQL and give options to connect to database (like --mysql passwd=PASS)',
    metavar='MYSQL_OPTION=VALUE',
    action='append')

parser.add_option(
    '--db-count',
    help='Number of databases (users) to connect to (default: %default)',
    metavar='NUMBER_OF_DATABASES',
    default=5,
    type='int')

parser.add_option(
    '--read',
    help='Frequency of reads (as opposed to writes, default: %default)',
    metavar='0.0-1.0',
    default=0.5,
    type='float')

parser.add_option(
    '--large-read',
    help='Among reads, how many should be over a large portion of the database (default: %default)',
    metavar='0.0-1.0',
    default=0.1,
    type='float')

parser.add_option(
    '--reps',
    help='How many repetitions of the test to run (default: %default)',
    metavar='COUNT',
    default=100,
    type='int')

parser.add_option(
    '--times',
    help='In each repetition, how many times to do it (default: %default)',
    metavar='COUNT',
    default=25,
    type='int')

parser.add_option(
    '--preload',
    help='Insert this many items before testing performance (default: %default)',
    metavar='NUMBER',
    default=1000,
    type='int')

parser.add_option(
    '--stream',
    action='store_true',
    help="Use the logdb.streamdb database instead of logdb.Database")

parser.add_option(
    '--profile',
    action='store_true',
    help="Use the profiler, print output after running")


def main():
    options, args = parser.parse_args()
    if options.mysql:
        kw = {}
        for arg in options.mysql:
            name, value = arg.split('=', 1)
            kw[name] = value
        kw['db'] = args[0]
        loader = MySQLStorage(**kw)
    else:
        dir = args[0]
        if not os.path.exists(dir):
            print 'Creating %s' % dir
            os.makedirs(dir)
        if options.stream:
            DatabaseConstructor = StreamDatabase
        else:
            DatabaseConstructor = Database

        def loader(name):
            return DatabaseConstructor(os.path.join(dir, name + '.db'))

    runner = Runner(db_count=options.db_count,
                    read_portion=options.read,
                    large_read_portion=options.large_read,
                    db_loader=loader)

    runner.preload(options.preload)
    if options.profile:
        import cProfile
        cProfile.run('runner.run_many(options.reps, options.times)')
    else:
        runner.run_many(options.reps, options.times)
    runner.counter.summarize()


if __name__ == '__main__':
    main()
