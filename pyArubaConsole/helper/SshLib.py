import paramiko
import os
from scp import SCPClient


class Ssh(object):
    _hostname = None
    _username = None
    _password = None

    def __init__(self):
        self.__ssh = paramiko.SSHClient()
        self.__ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    @property
    def hostname(self):
        return self._hostname

    @property
    def username(self):
        return self._username

    @property
    def password(self):
        return self._password

    @hostname.setter
    def hostname(self, value):
        self._hostname = value

    @username.setter
    def username(self, value):
        self._username = value

    @password.setter
    def password(self, value):
        self._password = value

    def __enter__(self):
        return self

    def connect(self):
        if self.hostname is None:
            raise 'Error, no hostname has been defined.'
        try:
            self.__ssh.connect(self.hostname, username=self.username, password=self.password)
        except Exception as exc:
            print {'status': 'ERR', 'error': exc}

    def run_command(self, cmd):
        try:
            stdin, stdout, stderr = self.__ssh.exec_command(cmd)
        except:
            return {'status': 'KO'}
        return {'status': 'OK', 'stdout': filter(None, stdout.read().split('\n'))}

    def put_file(self, source, destination):
        assert os.path.isfile(source)
        try:
            scp = SCPClient(self.__ssh.get_transport())
            scp.put(source, destination)
        except Exception as err:
            print err
            return {'status': 'KO'}
        return {'status': 'OK'}
