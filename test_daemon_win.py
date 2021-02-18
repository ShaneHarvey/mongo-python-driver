import subprocess

DETACHED_PROCESS = getattr(subprocess, 'DETACHED_PROCESS', 0x00000008)


def spawn_daemon(args):
    """Spawn a daemon process (Windows)."""
    popen = subprocess.Popen(args, creationflags=DETACHED_PROCESS)
    print('Running daemon process: ', popen.pid)
    del popen


def spawn_daemon_ignore_resource_warning(args):
    """Spawn a daemon process and ignore ResourceWarning (Windows)."""
    popen = subprocess.Popen(args, creationflags=DETACHED_PROCESS)
    print('Running daemon process: ', popen.pid)
    # Set the returncode to avoid erroneous warning:
    # "ResourceWarning: subprocess XXX is still running".
    # We use DETACHED_PROCESS to spawn the process as a daemon.
    popen.returncode = 0
    del popen


spawn_daemon(['sleep', '15'])
spawn_daemon_ignore_resource_warning(['sleep', '15'])
