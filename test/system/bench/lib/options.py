
from optparse import OptionParser
import logging

log = logging.getLogger("test.bench")

usage = "usage: %prog [options] [benchmark]"
parser = OptionParser(usage)
parser.add_option('-l', '--list', dest='list', action='store_true',
                  default=False)
parser.add_option('-v', '--level', dest='logLevel',
                  default=logging.INFO, type=int,
                  help="The logging level (%default)")
parser.add_option('-s', '--speed', dest='runSpeed', action='store', default='slow')
parser.add_option('-u', '--user', dest='user', action='store', default='')
parser.add_option('-p', '--password', dest='password', action='store', default='')
parser.add_option('-z', '--zookeepers', dest='zookeepers', action='store', default='')
parser.add_option('-i', '--instance', dest='instance', action='store', default='')



options, args = parser.parse_args()

__all__ = ['options', 'args']

