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

from multiprocessing import cpu_count
import grp
import os
import pwd

from .arbiter import Arbiter
from .daemon import daemonize, check_pidfile, remove_pidfile
from .log import init_log


def normalize_uid(uid):
    """Given a username or UID return UID as an integer."""
    if uid is not None:
        try:
            pent = pwd.getpwnam(uid)
        except KeyError:
            try:
                uid = int(uid)
            except ValueError:
                raise
        else:
            uid = pent.pw_uid
    else:
        uid = os.geteuid()
    return uid


def normalize_gid(gid):
    """Given a groupname or GID return GID as an integer."""
    if gid is not None:
        try:
            gent = grp.getgrnam(gid)
        except KeyError:
            try:
                gid = int(gid)
            except ValueError:
                raise
        else:
            gid = gent.gr_gid
    else:
        gid = os.getgid()
    return gid


def run(app, pidfile, logfile, name=None, no_daemon=False, uid=None,
        gid=None, num=None):
    """Run the given application.

    @param pidfile: Write the runners PID to this file.

    @param logfile: Write logs to this file.

    @param no_daemon: Do not daemonize.

    @param num: Number of instances to spawn.
    """
    check_pidfile(pidfile)

    if num is None:
        num = cpu_count()

    uid = normalize_uid(uid)
    gid = normalize_gid(gid)

    # Init logging before bootstrap to allow logging there. Don't use
    # logfile here since we don't want it to be owned by root.
    # TODO: Handle this better so initial logging doesn't go to stdout
    init_log(None)

    app.bootstrap()

    if not no_daemon:
        daemonize(pidfile)

    # Logging will be re-inited in arbiter
    logfile = None if logfile == '-' else logfile
    arbiter = Arbiter(app, logfile=logfile, name=name, uid=uid,
                      gid=gid, num_children=num)

    try:
        arbiter.run()
    finally:
        remove_pidfile(pidfile)
