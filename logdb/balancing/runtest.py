import os
import sys
import doctest

here = os.path.dirname(os.path.abspath(__file__))


def main():
    for name in sys.argv[1:]:
        name = os.path.join(name)
        if name.startswith('-'):
            continue
        options = doctest.ELLIPSIS
        if '-u' in sys.argv:
            options = options | doctest.REPORT_UDIFF
        doctest.testfile(name, optionflags=options)


if __name__ == '__main__':
    main()
