import subprocess

from lib import path
from lib import runner
from lib.options import log

    
def run(username, password, input):
    "Run a command in accumulo"
    handle = runner.start([path.accumulo('bin', 'accumulo'), 'shell -u %s -p %s' % (username, password) ],
                          stdin=subprocess.PIPE)
    log.debug("Running: %r", input)
    out, err = handle.communicate(input)
    log.debug("Process finished: %d (%s)",
              handle.returncode,
              ' '.join(handle.command))
    return handle.returncode, out, err
