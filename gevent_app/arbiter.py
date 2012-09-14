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

import errno
import gevent
from gevent.queue import Queue, Empty
from gevent.event import Event
import logging
from multiprocessing import Queue as LogQueue
import os
import select
import signal
import sys
import time
import tempfile
import traceback
import random

from . import util
from .log import LogConsumer, ChildLogHandler, StreamToLogger


FORMAT_STRING = '%(asctime)s [%(process)s] %(levelname)-6s [%(name)s] %(message)s'
DATE_FORMAT = "%Y-%m-%d %H:%M:%S%z"


log = logging.getLogger(__name__)


class HaltServer(Exception):
    """Halt the arbiter."""

    def __init__(self, reason, exit_status=1):
        self.reason = reason
        self.exit_status = exit_status
    
    def __str__(self):
        return "<HaltServer %r %d>" % (self.reason, self.exit_status)


class NotifyFile(object):
    """Abstraction of a channel that a child uses to signal to the
    arbiter that it is still alive.
    """

    def __init__(self, uid, gid):
        old_umask = os.umask(0) #cfg.get("umask", 0))
        fd, name = tempfile.mkstemp(prefix="gapp-")

        # allows the process to write to the file
        util.chown(name, uid, gid)
        os.umask(old_umask)

        # unlink the file so we don't leak tempory files
        try:
            os.unlink(name)
            self._tmp = os.fdopen(fd, 'w+b', 1)
        except:
            os.close(fd)
            raise

        self.spinner = 0

    def notify(self): 
        try:
            self.spinner = (self.spinner+1) % 2
            os.fchmod(self._tmp.fileno(), self.spinner)
        except AttributeError:
            # python < 2.6
            self._tmp.truncate(0)
            os.write(self._tmp.fileno(), "X")

    def fileno(self):
        return self._tmp.fileno()
       
    def close(self):
        return self._tmp.close()


class InstanceRunner(object):
    """Maintainer for a single instance."""

    def __init__(self, app, log_queue, name=None, ppid=0, timeout=30,
                 uid=None, gid=None):

        self.app = app
        self.log_queue = log_queue
        self.name = name or self.__class__.__name__
        self.ppid = ppid
        self.uid = uid or os.geteuid()
        self.gid = gid or os.getegid()
        self.timeout = timeout
        self.booted = False
        self.tmp = NotifyFile(uid, gid)
        self.shutdown = Event()
        self.log = logging.getLogger('child')

    def run(self):
        """Main-loop of the instance runner."""
        self.init_process()
        self.log.info("Booting child with pid: %d", os.getpid())

        self.app.start()
        self.booted = True

        while not self.shutdown.is_set():
            self.update_proc_title()
            if os.getppid() != self.ppid:
                # Parent changed - lets drop out
                break
            self.tmp.notify()
            self.shutdown.wait(1)

        self.app.stop()

    def update_proc_title(self):
        util._setproctitle('worker %s: %s' % (self.name, str(self.app),))

    def capture_stdout(self):
        """Setup so that stdout and stderr are sent to a logger."""
        sys.stdout = StreamToLogger(logging.getLogger(
                'sys.stdout'), logging.INFO)
        sys.stderr = StreamToLogger(logging.getLogger(
                'sys.stderr'), logging.ERROR)

    def init_process(self):
        """Initialize process."""
        random.seed() # FIXME: seed with pid?
        # Initialize logging.
        logger = logging.getLogger()
        logger.addHandler(ChildLogHandler(self.log_queue))
        for handler in logger.handlers:
            if not isinstance(handler, ChildLogHandler):
                logger.removeHandler(handler)
        self.capture_stdout()
        # Initialize the rest.
        util.set_owner_process(self.uid, self.gid)
        util.close_on_exec(self.tmp.fileno())
        self.init_signals()

    def init_signals(self):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        gevent.signal(signal.SIGQUIT, self.handle_quit)
        gevent.signal(signal.SIGTERM, self.handle_exit)
        gevent.signal(signal.SIGWINCH, self.handle_winch)
            
    def handle_quit(self):
        self.log.info('Received quit')
        self.shutdown.set()

    def handle_exit(self):
        self.log.info('Received exit')
        sys.exit(0)
        
    def handle_winch(self):
        # Ignore SIGWINCH in worker. Fixes a crash on OpenBSD.
        return


class BaseArbiter(object):
    _CHILDREN = {}

    # A flag indicating if a worker failed to to boot. If a worker
    # process exist with this error code, the arbiter will terminate.
    _WORKER_BOOT_ERROR = 3

    _SIGNALS = map(lambda x: getattr(signal, "SIG%s" % x),
                   "HUP QUIT INT TERM WINCH CHLD".split())
    _SIG_NAMES = dict(
        (getattr(signal, name), name[3:].lower()) for name in dir(signal)
        if name[:3] == "SIG" and name[3] != "_"
    )

    formatter = logging.Formatter(FORMAT_STRING, DATE_FORMAT)
    
    def __init__(self, app, name=None, logfile=None, timeout=30,
                 uid=None, gid=None):
        self.app = app
        self.name = name or self.__class__.__name__
        self.timeout = timeout
        self.pid = None
        self.child_age = 0
        self.stopping = False
        self.sigqueue = Queue()
        self.logfile = logfile
        self.uid = uid or os.geteuid()
        self.gid = gid or os.getegid()
        self.handler = None
        self.log_queue = LogQueue(-1)

    def capture_stdout(self):
        """Setup so that stdout and stderr are sent to a logger."""
        sys.stdout = StreamToLogger(logging.getLogger(
                'sys.stdout'), logging.INFO)
        sys.stderr = StreamToLogger(logging.getLogger(
                'sys.stderr'), logging.ERROR)

    def init_log(self):
        """Initialize logging."""
        root = logging.getLogger()

        for handler in root.handlers:
            root.removeHandler(handler)
            handler.close()

        try:
            newhandler = (logging.StreamHandler() if not self.logfile
                          else logging.FileHandler(self.logfile))
            self.handler = newhandler
            self.handler.setFormatter(self.formatter)
            # We accept everything from our children.
            root.setLevel(logging.DEBUG)
            root.addHandler(self.handler)
        except Exception, e:
            logging.error('failed initializing logging: %s', e)

    def init_process(self):
        """Initialize the arbiter process."""
        self.pid = os.getpid()
        self.init_signals()
        self.init_log()
        log.debug("Arbiter %s booted on %d", self.name, self.pid)

    def init_signals(self):
        """Initialize signal handling."""
        def _signal(sig):
            """Handle received signal."""
            self.sigqueue.put(sig)
        map(lambda s: gevent.signal(s, _signal, s), self._SIGNALS)

    def init_children_log(self):
        """Start consuming logs from the children."""
        LogConsumer(self.log_queue, logging.getLogger())

    def run(self):
        """Run the arbiter."""
        self.init_process()
        self.spawn_children()
        self.init_children_log()

        while True:
            try:
                self.reap_children()
                self.murder_children()
                self.manage_children()

                try:
                    sig = self.sigqueue.get(timeout=1)
                except Empty:
                    continue

                signame = self._SIG_NAMES.get(sig, None)
                if signame is None:
                    log.info("Ignoring unknown signal: %s", sig)
                    continue
                
                handler = getattr(self, "handle_%s" % signame, None)
                if not handler:
                    log.error("Unhandled signal: %s", signame)
                    continue

                log.info("Handling signal: %s", signame)
                handler()
            except StopIteration:
                self.halt()
            except KeyboardInterrupt:
                log.info("Received keyboard interrupt")
                self.halt()
            except HaltServer, inst:
                self.halt(reason=inst.reason, exit_status=inst.exit_status)
            except SystemExit:
                raise
            except Exception:
                log.info("Unhandled exception in main loop:\n%s",  
                            traceback.format_exc())
                self.stop(False)
                sys.exit(-1)

    def handle_hup(self):
        """HUP handling.

        - Reload configuration
        - Start the new worker processes with a new configuration
        - Gracefully shutdown the old worker processes

        """
        log.info("Hang up: %s", self.name)
        self.init_log()
        
    def handle_quit(self):
        """Quit."""
        raise StopIteration
    
    def handle_int(self):
        """Interrupt."""
        raise StopIteration
    
    def handle_term(self):
        """Terminate."""
        self.stop(True)
        raise StopIteration

    def handle_winch(self):
        """Window change."""
        if os.getppid() == 1 or os.getpgrp() != os.getpid():
            log.info("graceful stop of children")
            self.num_children = 0
            self.kill_children(signal.SIGQUIT)
        else:
            log.info("SIGWINCH ignored. Not daemonized")
    
    def halt(self, reason=None, exit_status=0):
        """Halt arbiter."""
        log.info("Shutting down: %s", self.name)
        if reason is not None:
            log.info("Reason: %s", reason)
        self.stop()
        log.info("See you next")
        sys.exit(exit_status)
        
    def stop(self, graceful=True):
        """Stop children.
        
        @param graceful: If True children will be killed gracefully
            (ie. trying to wait for the current connection)
        """
        self.stopping = True
        sig = signal.SIGQUIT if graceful else signal.SIGTERM
        self.kill_children(sig)
        limit = time.time() + self.timeout
        while True:
            if time.time() >= limit or not self._CHILDREN:
                break
            gevent.sleep(0.1)
            self.reap_children()
        self.kill_children(signal.SIGKILL)   
        self.stopping = False

    def murder_children(self):
        """Kill unused/idle children."""
        for (pid, child_info) in self._CHILDREN.items():
            (child, state) = child_info
            if state and child.timeout is not None:
                try:
                    diff = time.time() - os.fstat(child.tmp.fileno()).st_ctime
                    if diff <= child.timeout:
                        continue
                except ValueError:
                    continue
            elif state and child.timeout is None:
                continue

            log.critical("WORKER TIMEOUT (pid:%s)", pid)
            self.kill_worker(pid, signal.SIGKILL)
        
    def reap_children(self):
        """Reap children to avoid zombie processes."""
        try:
            while True:
                wpid, status = os.waitpid(-1, os.WNOHANG)
                if not wpid:
                    break
                
                # A worker said it cannot boot. We'll shutdown
                # to avoid infinite start/stop cycles.
                exitcode = status >> 8
                if exitcode == self._WORKER_BOOT_ERROR:
                    reason = "Worker failed to boot."
                    raise HaltServer(reason, self._WORKER_BOOT_ERROR)

                child_info = self._CHILDREN.pop(wpid, None)
                if not child_info:
                    continue

                child, state = child_info
                child.tmp.close()
        except OSError, e:
            if e.errno == errno.ECHILD:
                pass
    
    def manage_children(self):
        """Maintain the number of children by spawning or killing as
        required.
        """

    def spawn_child(self):
        """Spawn a child."""
        try:
            child = InstanceRunner(self.app, self.log_queue,
               ppid=self.pid, timeout=self.timeout, name=self.name,
               uid=self.uid, gid=self.gid)
        except:
            log.info("Unhandled exception while creating '%s':\n%s",  
                            self.name, traceback.format_exc())
            return

        pid = gevent.fork()
        if pid != 0:
            self._CHILDREN[pid] = (child, 1)
            return

        # Process Child
        worker_pid = os.getpid()
        try:
            child.run()
            sys.exit(0)
        except SystemExit:
            raise
        except:
            log.exception("Exception in worker process:")
            if not child.booted:
                sys.exit(self._WORKER_BOOT_ERROR)
            sys.exit(-1)
        finally:
            log.info("Instance exiting (pid: %s)", worker_pid)
            try:
                child.tmp.close()
            except:
                pass

    def spawn_children(self):
        """Spawn new children as needed.
        
        This is where a worker process leaves the main loop
        of the master process.
        """
        for child in self._CHILDREN_SPECS: 
            self.spawn_child(child)

    def kill_children(self, sig):
        """Kill all children with the signal C{sig}."""
        for pid in self._CHILDREN.keys():
            self.kill_worker(pid, sig)

    def kill_worker(self, pid, sig):
        """Kill a worker."""
        try:
            os.kill(pid, sig)
        except OSError, e:
            if e.errno == errno.ESRCH:
                try:
                    (child, info) = self._CHILDREN.pop(pid)
                    child.tmp.close()
                    return
                except (KeyError, OSError):
                    return
            raise            


class Arbiter(BaseArbiter):
    """Arbiter that maintains a pool of children."""

    def __init__(self, app, num_children=1, **kwargs):
        BaseArbiter.__init__(self, app, **kwargs)
        self.num_children = num_children

    def init_process(self):
        BaseArbiter.init_process(self)
        util._setproctitle("arbiter [%s running %s children]" % (self.name,  
            self.num_children))
        
    def manage_children(self):
        """Maintain the number of children by spawning or killing as
        required.
        """
        if len(self._CHILDREN.keys()) < self.num_children:
            self.spawn_children()

    def spawn_children(self):
        """Spawn new children as needed."""
        for i in range(self.num_children - len(self._CHILDREN.keys())):
            self.spawn_child()
