# Copyright 2012 Johan Rydberg.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import gevent
import os
import sys


class AlreadyRunningError(Exception):
    pass


def write_pidfile(pidfile):
    """Create and write to pidfile."""
    pid = os.getpid()
    with open(pidfile, 'w') as fp:
        fp.write(str(pid))


def remove_pidfile(pidfile):
    """Remove pidfile if it exists."""
    if os.path.exists(pidfile):
        os.remove(pidfile)


def check_pidfile(pidfile):
    """Check existance and contents of pidfile.

    Will remove stale pidfiles.
    """
    if os.path.exists(pidfile):
        with open(pidfile) as fp:
            pid = int(fp.read().strip())
        try:
            os.kill(pid, 0)
        except OSError:
            print "Removing stale pidfile."
            remove_pidfile(pidfile)
        else:
            raise AlreadyRunningError()


def daemonize(pidfile, stdin='/dev/null', stdout='/dev/null',
              stderr='/dev/null'):
    """Creates a child process, shuts down the parent process, and
    then redirects the io streams to a log file or /dev/null.
    """
    # Do first fork.
    try:
        pid = gevent.fork()
        if pid != 0:
            os._exit(0)
    except OSError, exc:
        # Python raises OSError rather than returning negative numbers.
        sys.exit("%s: fork #1 failed: (%d) %s\n"
                 % (sys.argv[0], exc.errno, exc.strerror))
        
    os.setsid()

    # Do second fork
    try:
        pid = gevent.fork()
        if pid > 0:
            # Second parent.
            os._exit(0)
    except OSError, exc:
        sys.exit("%s: fork #2 failed: (%d) %s\n"
                 % (sys.argv[0], exc.errno, exc.strerror))

    #os.chdir("/")

    si = open(stdin, "r")
    so = open(stdout, "a+")
    se = open(stderr, "a+", 0)

    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())

    write_pidfile(pidfile)
