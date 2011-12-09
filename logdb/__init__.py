import os
import posixfile
import struct

int_encoding = struct.Struct('<I')
pair_encoding = struct.Struct('<II')


class Database(object):

    INDEX_SIZE = 100

    def __init__(self, filename):
        ## FIXME: a race condition here:
        exists = os.path.exists(filename)
        self.fp = posixfile.open(filename, 'r+b')
        if not exists:
            self.fp.write(pair_encoding.pack(0, 0))
            self.fp.write('\x00' * (self.INDEX_SIZE * 8))

    def extend(self, datas):
        """Appends the data to the database, returning the integer
        counter for the first item in the data
        """
        if not datas:
            raise Exception('You must give some data')
        self.fp.lock('w|', 5, 5, posixfile.SEEK_END)
        self.fp.seek(5, posixfile.SEEK_END)
        start_pos = self.fp.tell()
        count = int_encoding.unpack(self.fp.read(4))
        self.fp.read(1)
        start = count + 1
        written_length = 0
        for data in datas:
            count += 1
            assert isinstance(data, str)
            enc_data = int_encoding.pack(len(data) + 5) + data + int_encoding.pack(count) + '\xff'
            written_length += enc_data
            self.fp.write(enc_data)
        self.fp.lock('u', start_pos)
        self.fp.lock('w|', 0, 4, posixfile.SEEK_SET)
        self.fp.seek(0)
        index_start, index_end = pair_encoding.unpack(self.fp.read(8))
        self.fp.seek(index_end * 8 + 8)
        self.fp.write(pair_encoding.pack(start, start_pos + 5))
        next_end = (index_end + 1) % self.INDEX_SIZE
        next_start = index_start
        if next_end == index_start:
            # We need to move start forward
            next_start += (index_start + 1) % self.INDEX_SIZE
        self.fp.seek(0)
        self.fp.write(pair_encoding.pack(index_start, index_end))
        if next_end == index_start:
            self.fp.seek(index_start * 8 + 8)
            # Relying a little on atomicity here:
            self.fp.write(pair_encoding.pack(0, 0))
        self.fp.lock('u', 0, 4, posixfile.SEEK_SET)
        return start

    def read(self, seek, length=-1):
        """Read items from the database, starting from count, max
        length (or -1 means to the end)
        """
        self.fp.seek(0)
        start, end = pair_encoding.unpack(self.fp.read(8))
        self.fp.seek(start)
        index_pos = start
        best = 8 + self.INDEX_SIZE * 8
        while 1:
            if end < start:
                if index_pos >= end and index_pos < start:
                    break
            else:
                if index_pos >= end:
                    break
            count, pos = pair_encoding.unpack(self.fp.read(8))
            if count == 0 and pos == 0:
                # We encountered a blank/invalid record
                continue
            if count > seek:
                break
            best = pos
        pos = best
        while 1:
            self.fp.seek(pos)
            size = int_encoding.unpack(self.fp.read(4))
            self.fp.seek(pos + size - 5)
            count = int_encoding.unpack(self.fp.read(4))
            if count == seek:
                self.fp.seek(pos)
                break
            pos += size + 4
        while length < -1 or length > 0:
            count_size = self.fp.read(8)
            if len(count_size) < 8:
                break
            count, size = pair_encoding.unpack(count_size)
            data = self.fp.read(size - 1)
            last = self.fp.read(1)
            if not last:
                break
            if last != '\xff':
                raise Exception('Invalid data: %r' % data)
            yield count, data
            length -= 1
