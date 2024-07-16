#!/bin/python
# 2024 by Ton Smeele, Utrecht University
#

import pickle


class Pathdb(object):
    """
    Pathdb maintains an in-memory database of pathnames.
    """
    def __init__(self):
        self._root = {}

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        out = ''
        for item in self.iter():
            out = out + item + '\n'
        return out

    def iter(self):
        yield from self._visit('/', self._root)

    def _visit(self, prefix, path):
        for item in path:
            if path[item] != {}:
                yield from self._visit(prefix + item + '/', path[item])
            else:
                yield prefix + item

    def size(self):
        count = 0
        for item in self.iter():
            count = count + 1
        return count

    def _components(self, path):
        parts = path.split('/')
        if parts[0] == '':
            # if path started with '/' it will have an extra empty component
            return parts[1:]
        return parts

    def register(self, path):
        if path is None or path == '':
            return
        parts = self._components(path)
        level = self._root
        for part in parts:
            if part not in level:
                level[part] = {}
            level = level[part]

    def is_registered(self, path):
        if path is None or path == '':
            return False
        parts = self._components(path)
        level = self._root
        for part in parts:
            if part not in level:
                return False
            level = level[part]
        return True

    def dump(self, filename):
        with open(filename, 'wb') as f:
            pickle.dump(self._root, f)

    def load(self, filename):
        with open(filename, 'rb') as f:
            self._root = pickle.load(f)


if __name__ == "__main__":
    # unit test and usage example
    db = Pathdb()
    db.register('/a')
    db.register('/a/b')
    db.register('/a/c')
    db.register('/d')
    print(db)
    print(db.size())
    for item in db.iter():
        print('visit: {}'.format(item))
    print('dump db in file')
    db.dump('/tmp/pathdb-dumpfile.bin')
    print('empty db')
    db.root = {}
    print(db)
    print('load db from file')
    db.load('/tmp/pathdb-dumpfile.bin')
    print(db)
