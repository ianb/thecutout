import os
import sys
import doctest

here = os.path.dirname(os.path.abspath(__file__))


def main():
    for name in sys.argv[1:]:
        name = os.path.join(name)
        doctest.testfile(name, optionflags=doctest.ELLIPSIS | doctest.REPORT_UDIFF)


if __name__ == '__main__':
    main()
