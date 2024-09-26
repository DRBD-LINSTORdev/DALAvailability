from prettytable import PrettyTable
from utils import exec_command
import logging
import logging.handlers
import datetime
import sys
import socket

def get_host_ip():
    """
    查询本机ip地址
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip


def exec_cmd(cmd, conn=None):
    local_obj = exec_command.LocalProcess()
    if conn:
        result = conn.exec_cmd(cmd)
        log_data = f'{conn._host} - {cmd} - {result}'
        result = result.decode() if isinstance(result, bytes) else result
    else:
        result = local_obj.exec_cmd(cmd)
        log_data = f'{get_host_ip()} - {cmd} - {result}'
        result = result.decode() if isinstance(result, bytes) else result
    # result = result.decode() if isinstance(result, bytes) else result
    # log_data = f'{get_host_ip()} - {cmd} - {result}'
    Log().logger.info(log_data)
    if result['st']:
        pass
    if result['st'] is False:
        print(result['rt'])
        sys.exit()
    return result['rt']


class RWData(object):

    def dd_write(self,device_name, ssh_conn=None):
        cmd = f'dd if=/dev/urandom of={device_name} oflag=direct status=progress'
        result = exec_cmd(cmd,ssh_conn)
        return result


    def dd_read(self,device_name,read_test_path, ssh_conn=None):
        cmd = f"dd if={device_name} of={read_test_path} oflag=direct status=progress"
        result = exec_cmd(cmd,ssh_conn)
        return result


    def kill_dd(self,dd_pid, ssh_conn=None):
        cmd = f'kill {dd_pid}'
        result = exec_cmd(cmd,ssh_conn)
        return result



class Log(object):
    def __init__(self):
        pass

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            Log._instance = super().__new__(cls)
            Log._instance.logger = logging.getLogger()
            Log._instance.logger.setLevel(logging.INFO)
            Log.set_handler(Log._instance.logger)
        return Log._instance

    @staticmethod
    def set_handler(logger):
        now_time = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        file_name = str(now_time) + '.log'
        fh = logging.FileHandler(file_name, mode='a', encoding='utf-8')
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
        fh.setFormatter(formatter)
        logger.addHandler(fh)


class Table(object):
    def __init__(self,field_name):
        self.table = PrettyTable(field_name)

    def add_row(self, list_data):
        self.table.add_row(list_data)

    def print_table(self):
        print(self.table)