# coding=UTF-8
#!/usr/bin/env python
"""map.py"""

import sys

for line in sys.stdin:
    line = line.strip()
    users = line.split(',')
    print ('%s\t%s,%s' % (users[10],users[1], users[7]))
