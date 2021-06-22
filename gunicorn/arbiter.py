# -*- coding: utf-8 -
#
# This file is part of gunicorn released under the MIT license.
# See the NOTICE for more information.
import errno
import os
import random
import select
import signal
import sys
import time
import traceback

from gunicorn.errors import HaltServer, AppImportError
from gunicorn.pidfile import Pidfile
from gunicorn import sock, systemd, util

from gunicorn import __version__, SERVER_SOFTWARE


class Arbiter(object):
    """
    Arbiter maintain the workers processes alive. It launches or
    kills them if needed. It also manages application reloading
    via SIGHUP/USR2.
    """

    # A flag indicating if a worker failed to
    # to boot. If a worker process exist with
    # this error code, the arbiter will terminate.
    WORKER_BOOT_ERROR = 3

    # A flag indicating if an application failed to be loaded
    APP_LOAD_ERROR = 4
    # CO(lk): custom exit code, raised in .spawn_worker(), detected in .reap_workers()

    START_CTX = {}

    LISTENERS = []
    WORKERS = {}
    PIPE = []

    # I love dynamic languages
    SIG_QUEUE = []
    SIGNALS = [getattr(signal, "SIG%s" % x)
               for x in "HUP QUIT INT TERM TTIN TTOU USR1 USR2 WINCH".split()]
    SIG_NAMES = dict(
        (getattr(signal, name), name[3:].lower()) for name in dir(signal)
        if name[:3] == "SIG" and name[3] != "_"
    )

    def __init__(self, app):
        os.environ["SERVER_SOFTWARE"] = SERVER_SOFTWARE

        self._num_workers = None
        self._last_logged_active_worker_count = None
        self.log = None

        self.setup(app)

        self.pidfile = None
        self.systemd = False
        self.worker_age = 0
        # CO(lk): when create `reexec` child, save child pid in `.reexec_pid` for the parent
        self.reexec_pid = 0
        # CO(lk): normally 0. only set non-0 as parent pid for the arbiter child
        self.master_pid = 0
        self.master_name = "Master"

        cwd = util.getcwd()

        args = sys.argv[:]
        args.insert(0, sys.executable)

        # init start context
        self.START_CTX = {
            "args": args,
            "cwd": cwd,
            0: sys.executable
        }

    def _get_num_workers(self):
        return self._num_workers

    def _set_num_workers(self, value):
        old_value = self._num_workers
        self._num_workers = value
        self.cfg.nworkers_changed(self, value, old_value)
    num_workers = property(_get_num_workers, _set_num_workers)

    def setup(self, app):
        self.app = app
        self.cfg = app.cfg

        if self.log is None:
            self.log = self.cfg.logger_class(app.cfg)

        # reopen files
        # TODO(lk): GUNICORN_FD means the current process is `exec`ed from an old
        #  process. `Arbiter.log` is set to close on exec. Is this necessary?
        if 'GUNICORN_FD' in os.environ:
            self.log.reopen_files()

        self.worker_class = self.cfg.worker_class
        self.address = self.cfg.address
        self.num_workers = self.cfg.workers
        self.timeout = self.cfg.timeout
        self.proc_name = self.cfg.proc_name

        self.log.debug('Current configuration:\n{0}'.format(
            '\n'.join(
                '  {0}: {1}'.format(config, value.value)
                for config, value
                in sorted(self.cfg.settings.items(),
                          key=lambda setting: setting[1]))))

        # set enviroment' variables
        if self.cfg.env:
            for k, v in self.cfg.env.items():
                os.environ[k] = v
        # NOTE(lk): copy-on-write (COW), save ram, speed fork.
        #  Drawback: can't reload app but only config file
        if self.cfg.preload_app:
            self.app.wsgi()

    def start(self):
        """\
        Initialize the arbiter. Start listening and set pidfile if needed.
        """
        self.log.info("Starting gunicorn %s", __version__)

        # NOTE(lk): GUNICORN_PID, GUNICORN_FD used internally,
        #  passed from parent process by Arbiter.reexec()
        # CO(lk): current process is `reexec`ed, share same "master_id" with parent
        #  use `.2` suffix to differentiate them
        if 'GUNICORN_PID' in os.environ:
            self.master_pid = int(os.environ.get('GUNICORN_PID'))
            self.proc_name = self.proc_name + ".2"
            self.master_name = "Master.2"

        self.pid = os.getpid()
        if self.cfg.pidfile is not None:
            pidname = self.cfg.pidfile
            if self.master_pid != 0:
                pidname += ".2"
            self.pidfile = Pidfile(pidname)
            self.pidfile.create(self.pid)
        self.cfg.on_starting(self)

        self.init_signals()

        # NOTE(lk): Use file descriptors, or host:port
        #  fd from systemd, fd from GUNICORN_FD
        #  (host, port) from Config.
        #  which could be confirmed in sock.create_sockets()
        if not self.LISTENERS:
            fds = None
            # CO(lk): fds sequences (start from 3) inherited from systemd socket activation
            listen_fds = systemd.listen_fds()
            if listen_fds:
                self.systemd = True
                fds = range(systemd.SD_LISTEN_FDS_START,
                            systemd.SD_LISTEN_FDS_START + listen_fds)

            elif self.master_pid:
                # CO(lk): if master_pid is not 0, the master is child `reexec`ed.
                fds = []
                # CO(lk): as a `reexec`ed child, used the same fd from parent
                for fd in os.environ.pop('GUNICORN_FD').split(','):
                    fds.append(int(fd))
            # NOTE(lk): only one of the type unix socket(fd), tcp socket(bind) is used
            self.LISTENERS = sock.create_sockets(self.cfg, self.log, fds)

        listeners_str = ",".join([str(l) for l in self.LISTENERS])
        self.log.debug("Arbiter booted")
        self.log.info("Listening at: %s (%s)", listeners_str, self.pid)
        self.log.info("Using worker: %s", self.cfg.worker_class_str)
        # CO(lk): notify systemd with socket conn. If run within systemd is detected
        systemd.sd_notify("READY=1\nSTATUS=Gunicorn arbiter booted", self.log)

        # check worker class requirements
        if hasattr(self.worker_class, "check_config"):
            self.worker_class.check_config(self.cfg, self.log)
        # CO(lk): call hook func "when_ready"
        self.cfg.when_ready(self)

    def init_signals(self):
        """\
        Initialize master signal handling. Most of the signals
        are queued. Child signals only wake up the master.
        """
        # close old PIPE
        for p in self.PIPE:
            os.close(p)

        # initialize the pipe
        self.PIPE = pair = os.pipe()
        for p in pair:
            util.set_non_blocking(p)
            # CO(lk): avoid fd leaking to forked processes
            util.close_on_exec(p)
        # CO(lk): close `FileHandler` on `exec`, avoid inherited by `exec`ed process
        self.log.close_on_exec()

        # initialize all signals
        for s in self.SIGNALS:
            signal.signal(s, self.signal)
        # CO(lk): SIGCHLD, notify parent one of its child process ended.
        #  processed directly without putting sig into queue
        signal.signal(signal.SIGCHLD, self.handle_chld)
        # TODO(lk):
        #  - `handle_chld()` separated without any explanation. 1/16/10 by Chesneau
        #  - why don't set `signal.set_wakeup_fd()` on Arbiter. Because of SIG_QUEUE?

    def signal(self, sig, frame):
        # CO(lk): don't handle the sig directly but save it to a queue
        #  wakeup child processes to handle it in main loop under Arbiter.run()
        if len(self.SIG_QUEUE) < 5:
            self.SIG_QUEUE.append(sig)
            # CO(lk): put sth into PIPE to avoid wait 1s to get obj from queue
            self.wakeup()

    def run(self):
        "Main master loop."
        # CO(lk): start(), init siginals, create LISTENERS
        self.start()
        util._setproctitle("master [%s]" % self.proc_name)

        try:
            # CO(lk): create or delete workers to meet worker number
            self.manage_workers()

            while True:
                # NOTE(lk): upgrade on the fly. kill the old arbiter and
                #  promote the new one.
                self.maybe_promote_master()
                # CO(lk): signal is not processed directly, but collected in queue
                sig = self.SIG_QUEUE.pop(0) if self.SIG_QUEUE else None
                if sig is None:
                    self.sleep()
                    # CO(lk): close workers timed out
                    self.murder_workers()
                    # CO(lk): create or delete workers to meet worker number
                    self.manage_workers()
                    continue

                if sig not in self.SIG_NAMES:
                    self.log.info("Ignoring unknown signal: %s", sig)
                    continue

                signame = self.SIG_NAMES.get(sig)
                handler = getattr(self, "handle_%s" % signame, None)
                if not handler:
                    self.log.error("Unhandled signal: %s", signame)
                    continue
                self.log.info("Handling signal: %s", signame)
                handler()
                # NOTE(lk): wakeup() let us skip sleep() timeout in next loop.
                #  Cause after handling current signal, worker status may be
                #  changed, need to manage_workers() as soon as possible.
                #  commit made by Chesneau in 2010-01-21 db01c21
                self.wakeup()
        except (StopIteration, KeyboardInterrupt):
            # NOTE(lk): handle_term, int, quit
            #  int, quit call Arbiter.stop(False) themselves. while term doesn't
            #  Defensive, in case stop(False) failed? Nope. Force quit workers.
            #  No way to differentiate them with only `StopIteration`.
            self.halt()
        except HaltServer as inst:
            # CO(lk): handle_chld() -> reap_workers()
            self.halt(reason=inst.reason, exit_status=inst.exit_status)
        except SystemExit:
            raise
        except Exception:
            self.log.info("Unhandled exception in main loop",
                          exc_info=True)
            self.stop(False)
            if self.pidfile is not None:
                self.pidfile.unlink()
            sys.exit(-1)

    def handle_chld(self, sig, frame):
        "SIGCHLD handling"
        # CO(lk): kill hanging workers. Since worker number may be changed,
        #  wakeup() to skip wait in sleep()
        self.reap_workers()
        self.wakeup()

    def handle_hup(self):
        """\
        HUP handling.
        - Reload configuration
        - Start the new worker processes with a new configuration
        - Gracefully shutdown the old worker processes
        """
        self.log.info("Hang up: %s", self.master_name)
        self.reload()

    def handle_term(self):
        "SIGTERM handling"
        raise StopIteration

    def handle_int(self):
        "SIGINT handling"
        self.stop(False)
        raise StopIteration

    def handle_quit(self):
        "SIGQUIT handling"
        self.stop(False)
        raise StopIteration

    def handle_ttin(self):
        """\
        SIGTTIN handling.
        Increases the number of workers by one.
        """
        self.num_workers += 1
        self.manage_workers()

    def handle_ttou(self):
        """\
        SIGTTOU handling.
        Decreases the number of workers by one.
        """
        if self.num_workers <= 1:
            return
        self.num_workers -= 1
        self.manage_workers()

    def handle_usr1(self):
        """\
        SIGUSR1 handling.
        Kill all workers by sending them a SIGUSR1
        """
        self.log.reopen_files()
        self.kill_workers(signal.SIGUSR1)

    def handle_usr2(self):
        """\
        SIGUSR2 handling.
        Creates a new arbiter/worker set as a fork of the current
        arbiter without affecting old workers. Use this to do live
        deployment with the ability to backout a change.
        """
        self.reexec()

    def handle_winch(self):
        """SIGWINCH handling"""
        if self.cfg.daemon:
            # CO(lk): equivalent to
            #  if os.getppid() == 1 or os.getpgrp() != os.getpid():
            #  The expected way to hot upgrade
            #  1. USR2, reexec a new arbiter
            #  2. WINCH, stop workers in the old arbiter
            #  3. TERM, kill and exit the old arbiter
            #  The new arbiter will promote itself automatically
            #  The problem is non-daemon arbiter can't handle WINCH. There's no
            #  way WINCH is sent by user, or emited by terminal after resizing.
            #  For non-daemon arbiter, stopping workers is delayed at TERM.
            #  So the question why WINCH is chosen to stop workers processing req?
            self.log.info("graceful stop of workers")
            self.num_workers = 0
            self.kill_workers(signal.SIGTERM)
        else:
            self.log.debug("SIGWINCH ignored. Not daemonized")

    def maybe_promote_master(self):
        # CO(lk): master_pid, if non 0, it's `reexec`ed child
        if self.master_pid == 0:
            return
        # NOTE(lk): once parent is killed, the child arbiter is managed
        #  by parent of parent arbiter. Promote the `reexec`ed child.
        if self.master_pid != os.getppid():
            self.log.info("Master has been promoted.")
            # reset master infos
            self.master_name = "Master"
            self.master_pid = 0
            self.proc_name = self.cfg.proc_name
            # CO(lk): GUNICORN_FD is cleaned in Arbiter.start()
            del os.environ['GUNICORN_PID']
            # rename the pidfile
            if self.pidfile is not None:
                self.pidfile.rename(self.cfg.pidfile)
            # reset proctitle
            util._setproctitle("master [%s]" % self.proc_name)

    def wakeup(self):
        """\
        Wake up the arbiter by writing to the PIPE
        """
        try:
            os.write(self.PIPE[1], b'.')
        except IOError as e:
            if e.errno not in [errno.EAGAIN, errno.EINTR]:
                raise

    def halt(self, reason=None, exit_status=0):
        """ halt arbiter """
        self.stop()
        self.log.info("Shutting down: %s", self.master_name)
        if reason is not None:
            self.log.info("Reason: %s", reason)
        if self.pidfile is not None:
            self.pidfile.unlink()
        self.cfg.on_exit(self)
        sys.exit(exit_status)

    def sleep(self):
        """\
        Sleep until PIPE is readable or we timeout.
        A readable PIPE means a signal occurred.
        """
        try:
            # CO(lk): wait read file descriptor ready with timeout 1s if no content
            ready = select.select([self.PIPE[0]], [], [], 1.0)
            if not ready[0]:
                return
            while os.read(self.PIPE[0], 1):
                pass
        except (select.error, OSError) as e:
            # TODO: select.error is a subclass of OSError since Python 3.3.
            error_number = getattr(e, 'errno', e.args[0])
            if error_number not in [errno.EAGAIN, errno.EINTR]:
                raise
        except KeyboardInterrupt:
            sys.exit()

    def stop(self, graceful=True):
        """\
        Stop workers

        :attr graceful: boolean, If True (the default) workers will be
        killed gracefully  (ie. trying to wait for the current connection)
        """
        unlink = (
            # CO(lk): master_pid should be 0, unless it's `reexec`ed child
            #  haven't been promoted.
            #  reexec_pid is non 0 in the old arbiter/master has not been killed
            #  after calling reexec.
            self.reexec_pid == self.master_pid == 0
            and not self.systemd
            and not self.cfg.reuse_port
        )
        sock.close_sockets(self.LISTENERS, unlink)

        # NOTE(lk):
        #  TERM, set Worker.alive = False, stop handling req
        #  QUIT, kill worker directly
        self.LISTENERS = []
        sig = signal.SIGTERM
        if not graceful:
            sig = signal.SIGQUIT
        limit = time.time() + self.cfg.graceful_timeout
        # instruct the workers to exit
        self.kill_workers(sig)
        # wait until the graceful timeout
        while self.WORKERS and time.time() < limit:
            time.sleep(0.1)
        # NOTE(lk): called kill_worker() twice to found out died worker,
        #  and pop them out of .WORKERS. Since the 1st sig to worker may be TERM,
        #  which don't kill worker, but just stop it handling new requests
        self.kill_workers(signal.SIGKILL)

    def reexec(self):
        """\
        Relaunch the master and workers.
        """
        if self.reexec_pid != 0:
            self.log.warning("USR2 signal ignored. Child exists.")
            return

        if self.master_pid != 0:
            self.log.warning("USR2 signal ignored. Parent exists.")
            return

        master_pid = os.getpid()
        self.reexec_pid = os.fork()
        if self.reexec_pid != 0:
            return

        self.cfg.pre_exec(self)
        # CO(lk): use env vars to pass pid, fds to the new `reexec`ed child process
        environ = self.cfg.env_orig.copy()
        environ['GUNICORN_PID'] = str(master_pid)

        if self.systemd:
            environ['LISTEN_PID'] = str(os.getpid())
            environ['LISTEN_FDS'] = str(len(self.LISTENERS))
        else:
            environ['GUNICORN_FD'] = ','.join(
                str(l.fileno()) for l in self.LISTENERS)

        os.chdir(self.START_CTX['cwd'])

        # exec the process using the original environment
        os.execvpe(self.START_CTX[0], self.START_CTX['args'], environ)
        # CO(lk): new process is not Worker, not added into `Arbiter.WORKERS`
        #  So killing the old master by TERM do nothing with the new arbiter

    def reload(self):
        old_address = self.cfg.address

        # reset old environment
        for k in self.cfg.env:
            if k in self.cfg.env_orig:
                # reset the key to the value it had before
                # we launched gunicorn
                os.environ[k] = self.cfg.env_orig[k]
            else:
                # delete the value set by gunicorn
                try:
                    del os.environ[k]
                except KeyError:
                    pass

        # reload conf
        self.app.reload()
        self.setup(self.app)
        # CO(lk): reload config(important if using conf file),
        #  reset some attrs on Arbiter like app, cfg
        # TODO(lk): reload doesn't expect log obj changed

        # reopen log files
        self.log.reopen_files()

        # do we need to change listener ?
        if old_address != self.cfg.address:
            # close all listeners
            for l in self.LISTENERS:
                l.close()
            # init new listeners
            self.LISTENERS = sock.create_sockets(self.cfg, self.log)
            listeners_str = ",".join([str(l) for l in self.LISTENERS])
            self.log.info("Listening at: %s", listeners_str)

        # do some actions on reload
        self.cfg.on_reload(self)

        # unlink pidfile
        if self.pidfile is not None:
            self.pidfile.unlink()

        # create new pidfile
        if self.cfg.pidfile is not None:
            self.pidfile = Pidfile(self.cfg.pidfile)
            self.pidfile.create(self.pid)

        # set new proc_name
        util._setproctitle("master [%s]" % self.proc_name)

        # spawn new workers
        for _ in range(self.cfg.workers):
            self.spawn_worker()
        # manage workers
        self.manage_workers()
        # CO(lk): create new workers no matter what, and close those old workers

    def murder_workers(self):
        """\
        Kill unused/idle workers
        """
        # TODO(lk): fix doc, kill workers timed out
        if not self.timeout:
            return
        workers = list(self.WORKERS.items())
        for (pid, worker) in workers:
            try:
                # CO(lk): ctime of Worker.tmp is updated by Worker.notify()
                #  before a req handling
                if time.time() - worker.tmp.last_update() <= self.timeout:
                    continue
            except (OSError, ValueError):
                continue
            # CO(lk): close workers timed out
            # TODO(lk): Worker.aborted, who set it
            if not worker.aborted:
                self.log.critical("WORKER TIMEOUT (pid:%s)", pid)
                worker.aborted = True
                self.kill_worker(pid, signal.SIGABRT)
            else:
                self.kill_worker(pid, signal.SIGKILL)

    def reap_workers(self):
        """\
        Reap workers to avoid zombie processes
        """
        # CO(lk): when SIGCHLD received, end hanging worker process.
        #  os.waitpid(-1), Finding out hanging children of current Arbiter
        #  > if pid is -1, the request pertains to any child of the cur process
        #  os.wait() returns a 16-bit number, whose low byte is the signal number
        #  that killed the process, and whose high byte is the exit status
        #  (if the signal number is zero);
        try:
            while True:
                wpid, status = os.waitpid(-1, os.WNOHANG)
                if not wpid:
                    break
                # CO(lk): if rexeced arbiter child is hanging
                if self.reexec_pid == wpid:
                    self.reexec_pid = 0
                else:
                    # A worker was terminated. If the termination reason was
                    # that it could not boot, we'll shut it down to avoid
                    # infinite start/stop cycles.
                    exitcode = status >> 8
                    if exitcode == self.WORKER_BOOT_ERROR:
                        reason = "Worker failed to boot."
                        raise HaltServer(reason, self.WORKER_BOOT_ERROR)
                    if exitcode == self.APP_LOAD_ERROR:
                        reason = "App failed to load."
                        raise HaltServer(reason, self.APP_LOAD_ERROR)
                    if os.WIFSIGNALED(status):
                        self.log.warning(
                            "Worker with pid %s was terminated due to signal %s",
                            wpid,
                            os.WTERMSIG(status)
                        )

                    worker = self.WORKERS.pop(wpid, None)
                    if not worker:
                        continue
                    worker.tmp.close()
                    # CO(lk): call hook func on child exit
                    self.cfg.child_exit(self, worker)
        except OSError as e:
            if e.errno != errno.ECHILD:
                raise

    def manage_workers(self):
        """\
        Maintain the number of workers by spawning or killing
        as required.
        """
        if len(self.WORKERS) < self.num_workers:
            self.spawn_workers()

        workers = self.WORKERS.items()
        # CO(lk): sort worker from older to newer for later kill_worker()
        workers = sorted(workers, key=lambda w: w[1].age)
        while len(workers) > self.num_workers:
            (pid, _) = workers.pop(0)
            self.kill_worker(pid, signal.SIGTERM)
            # CO(lk): TERM to do a graceful shutdown.
            #  pop worker out of list, without waiting it.

        active_worker_count = len(workers)
        if self._last_logged_active_worker_count != active_worker_count:
            self._last_logged_active_worker_count = active_worker_count
            self.log.debug("{0} workers".format(active_worker_count),
                           extra={"metric": "gunicorn.workers",
                                  "value": active_worker_count,
                                  "mtype": "gauge"})

    def spawn_worker(self):
        # CO(lk): worker age, or worker sequence passed to Worker init
        self.worker_age += 1
        # NOTE(lk): req handling timeout is half of the configured value
        #  https://github.com/benoitc/gunicorn/issues/1886
        #  Use half value to avoid race condition between
        #  Worker.notify() and Arbiter.murder_workers(), or say it in another way,
        #  between detecting ctime of Worker.tmp in Arbiter and
        #  updating ctime of Worker.tmp in Worker
        worker = self.worker_class(self.worker_age, self.pid, self.LISTENERS,
                                   self.app, self.timeout / 2.0,
                                   self.cfg, self.log)
        # CO(lk): call hook func "pre_fork"
        self.cfg.pre_fork(self, worker)
        pid = os.fork()
        if pid != 0:
            # CO(lk): master process, return
            worker.pid = pid
            self.WORKERS[pid] = worker
            return pid

        # Do not inherit the temporary files of other workers
        for sibling in self.WORKERS.values():
            sibling.tmp.close()

        # Process Child
        worker.pid = os.getpid()
        try:
            util._setproctitle("worker [%s]" % self.proc_name)
            self.log.info("Booting worker with pid: %s", worker.pid)
            # CO(lk): hook func "post_fork"
            self.cfg.post_fork(self, worker)
            # NOTE(lk): Worker.init_process(). Block here for handling requests
            worker.init_process()
            sys.exit(0)
        except SystemExit:
            raise
        except AppImportError as e:
            self.log.debug("Exception while loading the application",
                           exc_info=True)
            print("%s" % e, file=sys.stderr)
            sys.stderr.flush()
            sys.exit(self.APP_LOAD_ERROR)
        except Exception:
            self.log.exception("Exception in worker process")
            if not worker.booted:
                sys.exit(self.WORKER_BOOT_ERROR)
            sys.exit(-1)
        finally:
            self.log.info("Worker exiting (pid: %s)", worker.pid)
            try:
                worker.tmp.close()
                self.cfg.worker_exit(self, worker)
            except Exception:
                self.log.warning("Exception during worker exit:\n%s",
                                 traceback.format_exc())

    def spawn_workers(self):
        """\
        Spawn new workers as needed.

        This is where a worker process leaves the main loop
        of the master process.
        """

        for _ in range(self.num_workers - len(self.WORKERS)):
            self.spawn_worker()
            time.sleep(0.1 * random.random())

    def kill_workers(self, sig):
        """\
        Kill all workers with the signal `sig`
        :attr sig: `signal.SIG*` value
        """
        worker_pids = list(self.WORKERS.keys())
        for pid in worker_pids:
            self.kill_worker(pid, sig)

    def kill_worker(self, pid, sig):
        """\
        Kill a worker

        :attr pid: int, worker pid
        :attr sig: `signal.SIG*` value
         """
        # TODO(lk): typo
        # NOTE(lk): Sending signal to Worker by os.kill()
        #  `kill_worker()` called twice by `.stop()` to pop out died worker from
        #  `.WORKERS`, and close `WorkerTmp`
        try:
            os.kill(pid, sig)
        except OSError as e:
            # NOTE(lk): Arbiter.stop() pass sig twice to workers. The 2nd time
            #  pops out the worker from list.
            #  man kill [ESRCH]
            #  No process or process group can be found corresponding to that pid
            if e.errno == errno.ESRCH:
                try:
                    worker = self.WORKERS.pop(pid)
                    worker.tmp.close()
                    self.cfg.worker_exit(self, worker)
                    return
                except (KeyError, OSError):
                    return
            raise
