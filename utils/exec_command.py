import paramiko
import subprocess
import time


class SSHconn(object):
    def __init__(self, host, name, port=22, username="root", password=None, timeout=8):
        self._name = name
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self.timeout = timeout
        self.sshconnection = None
        self.ssh_conn()
        self.channel = self.sshconnection.invoke_shell()


    def ssh_conn(self):
        """
        SSH连接
        """
        try:
            conn = paramiko.SSHClient()
            conn.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            conn.connect(hostname=self._host,
                         username=self._username,
                         port=self._port,
                         password=self._password,
                         timeout=self.timeout,
                         look_for_keys=False,
                         allow_agent=False)
            self.sshconnection = conn
        except paramiko.AuthenticationException:
            print(f" Error SSH connection message of {self._host}")
        except Exception as e:
            print(f" Failed to connect {self._host}")

    def exec_cmd(self, command):
        if self.sshconnection:
            stdin, stdout, stderr = self.sshconnection.exec_command(command)
            result = stdout.read()
            err = stderr.read()
            result = result.decode() if isinstance(result, bytes) else result
            err = err.decode() if isinstance(err, bytes) else err
            if err is not None and err != "":
                return {"st": False, "rt": err}
            else:
                return {"st": True, "rt": result}

    def invoke_send_command(self, command):
        self.channel.send(f'{command}\n')

    def invoke_receive_output(self, sec):
        time.sleep(sec)
        output = self.channel.recv(9999)
        print(output.decode('utf-8'))

    def download(self, local, remote):
        """
        sftp下载文件
        """
        try:
            sftp_file = paramiko.SFTPClient.from_transport(self.transport)
            download_file = sftp_file.get(remotepath=remote, localpath=local)
            return {"st": True, "rt": "File downloaded successfully"}
        except Exception as e:
            return {"st": False, "rt": e}

    def upload(self, local, remote):
        """
        sftp上传文件
        """
        try:
            sftp_file = paramiko.SFTPClient.from_transport(self.transport)
            upload_file = sftp_file.put(remotepath=remote, localpath=local)
            return {"st": True, "rt": "File uploaded successfully"}
        except Exception as e:
            return {"st": False, "rt": e}


class LocalProcess(object):
    def exec_cmd(self,command):
        """
        命令执行
        """
        sub_conn = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        if sub_conn.returncode == 0:
            result = sub_conn.stdout
            return {"st": True, "rt": result}
        else:
            print(f"Can't to execute command: {command}")
            err = sub_conn.stderr
            print(f"Error message:{err}")
            return {"st": False, "rt": err}
