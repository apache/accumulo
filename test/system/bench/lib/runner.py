
import subprocess

from lib.options import log
    
def start(command, stdin=None):
    log.debug("Running %s", ' '.join(command))
    handle = subprocess.Popen(command,
                              stdin=stdin,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
    # remember the command for debugging
    handle.command = command
    return handle
