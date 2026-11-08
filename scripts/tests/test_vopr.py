"""Linux lifecycle and command-status regressions: python3 -m unittest discover -s scripts/tests."""
import ctypes
import errno
import importlib.util
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import unittest
from unittest.mock import patch


SCRIPTS = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("vopr_profile", SCRIPTS / "vopr-profile.py")
profile = importlib.util.module_from_spec(spec)
spec.loader.exec_module(profile)


@unittest.skipUnless(hasattr(os, "fork"), "requires Linux process APIs")
class ProfileTests(unittest.TestCase):
    def test_perf_permission_failure_reaps_waiting_child(self):
        pid = os.fork()
        if pid == 0:
            signal.pause()
            os._exit(99)
        try:
            with patch.object(profile.ctypes, "CDLL") as libc:
                libc.return_value.syscall.return_value = -1
                ctypes.set_errno(errno.EACCES)
                with self.assertRaisesRegex(SystemExit, os.strerror(errno.EACCES)):
                    profile.open_rings(pid)
            with self.assertRaises(ChildProcessError):
                os.waitpid(pid, os.WNOHANG)
        finally:
            try:
                os.kill(pid, signal.SIGKILL)
                os.waitpid(pid, 0)
            except ProcessLookupError:
                pass

    def test_failed_exec_exits_child_without_running_profiler(self):
        pid = os.fork()
        if pid == 0:
            try:
                with patch.object(profile.os, "fork", return_value=0), \
                     patch.object(profile.os, "read", return_value=b"x"), \
                     patch.object(profile.sys, "argv", ["profile", "out", "/missing-vopr-program"]):
                    profile.main()
            except BaseException:
                os._exit(99)
            os._exit(98)
        _, status = os.waitpid(pid, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 127)


@unittest.skipUnless(os.name == "posix", "requires Bash and Linux command utilities")
class SweepTests(unittest.TestCase):
    def test_launcher_failure_cannot_report_success(self):
        self.run_launcher_failure(False)

    def test_launcher_failure_still_stops_sampler_and_reports_failed_seed(self):
        self.run_launcher_failure(True)

    def run_launcher_failure(self, failed_seed):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            commands = root / "bin"
            commands.mkdir()
            # Cargo need only identify the executable; no build or real simulation runs here.
            fixtures = {
                "cargo": '#!/bin/bash\necho "Executable vopr ($TEST_BIN)"\n',
                "git": '#!/bin/bash\nif [[ "$*" == *rev-parse* ]]; then echo testcommit; fi\n',
                "xargs": '#!/bin/bash\nwhile [ ! -s "$SAMPLER_PID" ]; do /bin/sleep 0.01; done\nlogs="${!#}"\nif [ "$FAILED_SEED" = 1 ]; then touch "$logs/1.failed"; "$TEST_BIN" > "$logs/1.log"; fi\nexit 42\n',
                "sleep": '#!/bin/bash\necho "$PPID" > "$SAMPLER_PID"\nexec /bin/sleep 0.1 >/dev/null 2>&1\n',
                "vopr": '#!/bin/bash\necho "VOPR failed"\necho "now WARN expected failure"\n',
            }
            for name, contents in fixtures.items():
                path = commands / name
                path.write_text(contents)
                path.chmod(0o755)
            env = dict(os.environ, PATH=f"{commands}:{os.environ['PATH']}",
                       VOPR_REPORTS=str(root / "reports"), SAMPLER_PID=str(root / "sampler.pid"),
                       TEST_BIN=str(commands / "vopr"), FAILED_SEED="1" if failed_seed else "0",
                       TMPDIR=str(root))
            output = subprocess.run(["bash", str(SCRIPTS / "vopr.sh"), "sweep", "1"],
                                    env=env, capture_output=True, text=True, timeout=10)
            self.assertEqual(output.returncode, 1 if failed_seed else 42, output.stdout + output.stderr)
            if failed_seed:
                self.assertIn("seed 1 FAILED", output.stdout)
                self.assertTrue((root / "reports/testcommit-1.md").is_file(), output.stdout + output.stderr)
            pid = int((root / "sampler.pid").read_text())
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)


if __name__ == "__main__":
    unittest.main()
