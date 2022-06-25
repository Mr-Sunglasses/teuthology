from io import BytesIO
import os

import paramiko
import socket

from mock import MagicMock, patch
from pytest import raises
from _pytest.monkeypatch import MonkeyPatch

from teuthology.orchestra import run
from teuthology.exceptions import (CommandCrashedError, CommandFailedError,
                                   ConnectionLostError)

def set_buffer_contents(buf, contents):
    buf.seek(0)
    if isinstance(contents, bytes):
        buf.write(contents)
    elif isinstance(contents, (list, tuple)):
        buf.writelines(contents)
    elif isinstance(contents, str):
        buf.write(contents.encode())
    else:
        raise TypeError(
            "%s is a %s; should be a byte string, list or tuple" % (
                contents, type(contents)
            )
        )
    buf.seek(0)


class TestRun(object):
    def setup(self):
        self.start_patchers()

    def teardown(self):
        self.stop_patchers()

    def start_patchers(self):
        self.m_remote_process = MagicMock(wraps=run.RemoteProcess)
        self.patcher_remote_proc = patch(
            'teuthology.orchestra.run.RemoteProcess',
            self.m_remote_process,
        )
        self.m_channel = MagicMock(spec=paramiko.Channel)()
        """
        self.m_channelfile = MagicMock(wraps=paramiko.ChannelFile)
        self.m_stdin_buf = self.m_channelfile(self.m_channel())
        self.m_stdout_buf = self.m_channelfile(self.m_channel())
        self.m_stderr_buf = self.m_channelfile(self.m_channel())
        """
        class M_ChannelFile(BytesIO):
            channel = MagicMock(spec=paramiko.Channel)()

        self.m_channelfile = M_ChannelFile
        self.m_stdin_buf = self.m_channelfile()
        self.m_stdout_buf = self.m_channelfile()
        self.m_stderr_buf = self.m_channelfile()
        self.m_ssh = MagicMock()
        self.m_ssh.exec_command.return_value = (
            self.m_stdin_buf,
            self.m_stdout_buf,
            self.m_stderr_buf,
        )
        self.m_transport = MagicMock()
        self.m_transport.getpeername.return_value = ('name', 22)
        self.m_ssh.get_transport.return_value = self.m_transport
        self.patcher_ssh = patch(
            'teuthology.orchestra.connection.paramiko.SSHClient',
            self.m_ssh,
        )
        self.patcher_ssh.start()
        # Tests must start this if they wish to use it
        # self.patcher_remote_proc.start()

    def stop_patchers(self):
        # If this patcher wasn't started, it's ok
        try:
            self.patcher_remote_proc.stop()
        except RuntimeError:
            pass
        self.patcher_ssh.stop()

    def test_exitstatus(self):
        self.m_stdout_buf.channel.recv_exit_status.return_value = 0
        proc = run.run(
            client=self.m_ssh,
            args=['foo', 'bar baz'],
        )
        assert proc.exitstatus == 0

    def test_run_cwd(self):
        self.m_stdout_buf.channel.recv_exit_status.return_value = 0
        run.run(
            client=self.m_ssh,
            args=['foo_bar_baz'],
            cwd='/cwd/test',
        )
        self.m_ssh.exec_command.assert_called_with('(cd /cwd/test && exec foo_bar_baz)')

    def test_capture_stdout(self):
        output = 'foo\nbar'
        set_buffer_contents(self.m_stdout_buf, output)
        self.m_stdout_buf.channel.recv_exit_status.return_value = 0
        stdout = BytesIO()
        proc = run.run(
            client=self.m_ssh,
            args=['foo', 'bar baz'],
            stdout=stdout,
        )
        assert proc.stdout is stdout
        assert proc.stdout.read().decode() == output
        assert proc.stdout.getvalue().decode() == output

    def test_capture_stderr_newline(self):
        output = 'foo\nbar\n'
        set_buffer_contents(self.m_stderr_buf, output)
        self.m_stderr_buf.channel.recv_exit_status.return_value = 0
        stderr = BytesIO()
        proc = run.run(
            client=self.m_ssh,
            args=['foo', 'bar baz'],
            stderr=stderr,
        )
        assert proc.stderr is stderr
        assert proc.stderr.read().decode() == output
        assert proc.stderr.getvalue().decode() == output

    def test_status_bad(self):
        self.m_stdout_buf.channel.recv_exit_status.return_value = 42
        with raises(CommandFailedError) as exc:
            run.run(
                client=self.m_ssh,
                args=['foo'],
            )
        assert str(exc.value) == "Command failed on name with status 42: 'foo'"

    def test_status_bad_nocheck(self):
        self.m_stdout_buf.channel.recv_exit_status.return_value = 42
        proc = run.run(
            client=self.m_ssh,
            args=['foo'],
            check_status=False,
        )
        assert proc.exitstatus == 42

    def test_status_crash(self):
        self.m_stdout_buf.channel.recv_exit_status.return_value = -1
        with raises(CommandCrashedError) as exc:
            run.run(
                client=self.m_ssh,
                args=['foo'],
            )
        assert str(exc.value) == "Command crashed: 'foo'"

    def test_status_crash_nocheck(self):
        self.m_stdout_buf.channel.recv_exit_status.return_value = -1
        proc = run.run(
            client=self.m_ssh,
            args=['foo'],
            check_status=False,
        )
        assert proc.exitstatus == -1

    def test_status_lost(self):
        m_transport = MagicMock()
        m_transport.getpeername.return_value = ('name', 22)
        m_transport.is_active.return_value = False
        self.m_stdout_buf.channel.recv_exit_status.return_value = -1
        self.m_ssh.get_transport.return_value = m_transport
        with raises(ConnectionLostError) as exc:
            run.run(
                client=self.m_ssh,
                args=['foo'],
            )
        assert str(exc.value) == "SSH connection to name was lost: 'foo'"

    def test_status_lost_socket(self):
        m_transport = MagicMock()
        m_transport.getpeername.side_effect = socket.error
        self.m_ssh.get_transport.return_value = m_transport
        with raises(ConnectionLostError) as exc:
            run.run(
                client=self.m_ssh,
                args=['foo'],
            )
        assert str(exc.value) == "SSH connection was lost: 'foo'"

    def test_status_lost_nocheck(self):
        m_transport = MagicMock()
        m_transport.getpeername.return_value = ('name', 22)
        m_transport.is_active.return_value = False
        self.m_stdout_buf.channel.recv_exit_status.return_value = -1
        self.m_ssh.get_transport.return_value = m_transport
        proc = run.run(
            client=self.m_ssh,
            args=['foo'],
            check_status=False,
        )
        assert proc.exitstatus == -1

    def test_status_bad_nowait(self):
        self.m_stdout_buf.channel.recv_exit_status.return_value = 42
        proc = run.run(
            client=self.m_ssh,
            args=['foo'],
            wait=False,
        )
        with raises(CommandFailedError) as exc:
            proc.wait()
        assert proc.returncode == 42
        assert str(exc.value) == "Command failed on name with status 42: 'foo'"

    def test_stdin_pipe(self):
        self.m_stdout_buf.channel.recv_exit_status.return_value = 0
        proc = run.run(
            client=self.m_ssh,
            args=['foo'],
            stdin=run.PIPE,
            wait=False
        )
        assert proc.poll() == 0
        code = proc.wait()
        assert code == 0
        assert proc.exitstatus == 0

    def test_stdout_pipe(self):
        self.m_stdout_buf.channel.recv_exit_status.return_value = 0
        lines = [b'one\n', b'two', b'']
        set_buffer_contents(self.m_stdout_buf, lines)
        proc = run.run(
            client=self.m_ssh,
            args=['foo'],
            stdout=run.PIPE,
            wait=False
        )
        assert proc.poll() == 0
        assert proc.stdout.readline() == lines[0]
        assert proc.stdout.readline() == lines[1]
        assert proc.stdout.readline() == lines[2]
        code = proc.wait()
        assert code == 0
        assert proc.exitstatus == 0

    def test_stderr_pipe(self):
        self.m_stdout_buf.channel.recv_exit_status.return_value = 0
        lines = [b'one\n', b'two', b'']
        set_buffer_contents(self.m_stderr_buf, lines)
        proc = run.run(
            client=self.m_ssh,
            args=['foo'],
            stderr=run.PIPE,
            wait=False
        )
        assert proc.poll() == 0
        assert proc.stderr.readline() == lines[0]
        assert proc.stderr.readline() == lines[1]
        assert proc.stderr.readline() == lines[2]
        code = proc.wait()
        assert code == 0
        assert proc.exitstatus == 0

    def test_copy_and_close(self):
        run.copy_and_close(None, MagicMock())
        run.copy_and_close('', MagicMock())
        run.copy_and_close(b'', MagicMock())


class TestQuote(object):
    def test_quote_simple(self):
        got = run.quote(['a b', ' c', 'd e '])
        assert got == "'a b' ' c' 'd e '"

    def test_quote_and_quote(self):
        got = run.quote(['echo', 'this && is embedded', '&&',
                         'that was standalone'])
        assert got == "echo 'this && is embedded' '&&' 'that was standalone'"

    def test_quote_and_raw(self):
        got = run.quote(['true', run.Raw('&&'), 'echo', 'yay'])
        assert got == "true && echo yay"


class TestRaw(object):
    def test_eq(self):
        str_ = "I am a raw something or other"
        raw = run.Raw(str_)
        assert raw == run.Raw(str_)


class TestErrorScanner(object):
    def setup(self):
        self.nose_failure = {
            'error_line': "2022-06-20T13:47:26.709 INFO:teuthology.orchestra.run.smithi119.\
                stderr:ERROR: s3tests_boto3.functional.test_s3.test_bucket_policy_put_obj_s3_noenc",
            'prev_detected_line': "teuthology.exceptions.UnitTestError: nose test failed \
                (s3 tests against rgw) on smithi119 with status 1: 'ERROR: s3tests_boto3.\
                functional.test_s3.test_bucket_policy_put_obj_s3_noenc'",
            'error_msg': 'ERROR: s3tests_boto3.functional.test_s3.test_bucket_policy_put_obj_s3_noenc',
        }
        self.no_error_line = "2022-06-20T13:37:30.690 INFO:teuthology.orchestra.run.smithi100.\
                stdout:Transaction check succeeded."
        self.gtest_failure = {
            'error_line': "2022-06-22T16:49:01.900 INFO:tasks.workunit.client.0.smithi102. \
                stdout:[  FAILED  ] TestClsRbd.get_all_features (0 ms)",
            'prev_detected_line': "teuthology.exceptions.UnitTestError: gtest test failed \
                (workunit test cls/test_cls_rbd.sh) on smithi102 with status 1: \
                '[  FAILED  ] TestClsRbd.get_all_features (0 ms)'",
            'error_msg': '[  FAILED  ] TestClsRbd.get_all_features (0 ms)',
        }
        self.monkeypatch = MonkeyPatch()

    def test_search_error_nose(self):
        error_line = self.nose_failure['error_line']
        no_error_line = self.no_error_line
        prev_detected_line = self.nose_failure['prev_detected_line']
        assert run.ErrorScanner()._search_error(line=error_line, test="nose") == self.nose_failure['error_msg']
        assert run.ErrorScanner()._search_error(line=no_error_line, test="nose") == None
        assert run.ErrorScanner()._search_error(line=prev_detected_line, test="nose") == None

    def test_search_error_gtest(self):
        error_line = self.gtest_failure['error_line']
        no_error_line = self.no_error_line
        prev_detected_line = self.gtest_failure['prev_detected_line']
        assert run.ErrorScanner()._search_error(line=error_line, test="gtest") == self.gtest_failure['error_msg']
        assert run.ErrorScanner()._search_error(line=no_error_line, test="gtest") == None
        assert run.ErrorScanner()._search_error(line=prev_detected_line, test="gtest") == None

    def test_is_prev_detected_error(self):
        prev_error = self.nose_failure['prev_detected_line']
        not_prev_error = self.nose_failure['error_line']
        assert run.ErrorScanner()._is_prev_detected_error(line=prev_error) == True
        assert run.ErrorScanner()._is_prev_detected_error(line=not_prev_error) == False

    def test_scan_nose_failure(self):
        logfile = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "log_files/test_scan_nose.log"
        )
        self.monkeypatch.setattr(run.ErrorScanner, '_logfile', logfile)
        self.monkeypatch.setattr(run.ErrorScanner, '_flag', 0)
        scanner = run.ErrorScanner()
        scan_result = scanner.scan(test_names=["nose"])
        assert scan_result == ('nose', self.nose_failure['error_msg'])

    def test_scan_gtest_failure(self):
        logfile = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "log_files/test_scan_gtest.log"
        )
        self.monkeypatch.setattr(run.ErrorScanner, '_logfile', logfile)
        self.monkeypatch.setattr(run.ErrorScanner, '_flag', 0)
        scanner = run.ErrorScanner()
        scan_result = scanner.scan(test_names=["gtest"])
        assert scan_result == ('gtest', self.gtest_failure['error_msg'])
