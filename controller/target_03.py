import sys
import traceback
import time
import subprocess
import re
import ctypes
import inspect
import gevent
import threading
#import utils
#import exec_command
from DALAvailability import utils
from DALAvailability import exec_command
import re
#import config_file
from DALAvailability import config_file
from threading import Thread

global _GLOBAL_DICT
_GLOBAL_DICT = {}

global _TIMES
_TIMES = 0

global _LOGGER
_LOGGER = None

def get_global_dict_value(key):
    try:
        return _GLOBAL_DICT[key]
    except KeyError:
        # print("KeyError of global value.")
        return utils.get_host_ip()

def _async_raise(tid, exctype):
    """raises the exception, performs cleanup if needed"""
    print('', f"Stop thread ...", 0)
    tid = ctypes.c_long(tid)
    if not inspect.isclass(exctype):
        exctype = type(exctype)
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))
    if res == 0:
        print("invalid thread id")
        # raise ValueError("invalid thread id")
    elif res != 1:
        # """if it returns a number greater than one, you're in trouble,
        # and you should call it again with exc=NULL to revert the effect"""
        ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
        print("PyThreadState_SetAsyncExc failed")
        # raise SystemError("PyThreadState_SetAsyncExc failed")

def re_search(conn, re_string, tgt_string, output_type='bool'):
    re_obj = re.compile(re_string)
    re_result = re_obj.search(tgt_string)
    if re_result:
        if output_type == 'bool':
            re_result = True
        if output_type == 'groups':
            re_result = re_result.groups()
        if output_type == 'group':
            re_result = re_result.group()
    return re_result

def check_drbd_status(conn, result, resource):
    re_stand_alone = f'connection:StandAlone'
    re_string = f'{resource}\s*role:(\w+).*\s*disk:(\w+)'
    # re_peer_string = '\S+\s*role:(\w+).*\s*peer-disk:(\w+)'
    if result:
        re_stand_alone_result = re_search(conn, re_stand_alone, result, "bool")
        if re_stand_alone_result:
            return 'StandAlone'
        re_result = re_search(conn, re_string, result, "groups")
        return re_result

def get_dd_pid(conn, device, result):
    re_string = f'\w*\s*(\d+)\s*.*dd if=/dev/urandom of={device} oflag=direct status=progress'
    re_result = re_search(conn, re_string, result, "groups")
    if re_result:
        return re_result[0]

def check_dd(conn, device):
    dd_node = RWData(conn)
    result = dd_node.get_dd()
    pid = get_dd_pid(conn, device, result)
    if pid:
        print(f"dd operation (pid: {pid}) is still in progress on {get_global_dict_value(conn)}")
        return True

def kill_dd(conn, device):
    dd_node = RWData(conn)
    result = dd_node.get_dd()
    pid = get_dd_pid(conn, device, result)
    if pid:
        dd_node.kill_dd(pid)
        print(f"Kill dd on {get_global_dict_value(conn)}.")
    else:
        print(f"dd operation had been finished on {get_global_dict_value(conn)}.")

class Connect(object):
    """
    通过ssh连接节点，生成连接对象的列表
    """
    list_vplx_ssh = []

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            Connect._instance = super().__new__(cls)
            Connect._instance.config = args[0]
            Connect.get_ssh_conn(Connect._instance)
        return Connect._instance

    def get_ssh_conn(self):
        local_ip = utils.get_host_ip()
        vplx_configs = self.config["versaplx"]
        username = "root"
        for vplx_config in vplx_configs:
            if "username" in vplx_config.keys():
                if vplx_config['username'] is not None:
                    username = vplx_config['username']
            if local_ip == vplx_config['public_ip']:
                self.list_vplx_ssh.append(None)
                _GLOBAL_DICT[None] = vplx_config['public_ip']
            else:
                ssh_conn = exec_command.SSHconn(vplx_config['public_ip'], vplx_config['port'], username,
                                         vplx_config['password'])
                self.list_vplx_ssh.append(ssh_conn)
                _GLOBAL_DICT[ssh_conn] = vplx_config['public_ip']

class Stor(object):
    def __init__(self, conn=None):
        self.conn = conn

    def get_drbd_status(self, resource):
        cmd = f'drbdadm status {resource}'
        result = utils.exec_cmd(cmd, self.conn)
        if result:
            return result

    def check_drbd_quorum(self, resource):
        cmd = f'drbdsetup show {resource}'
        result = utils.exec_cmd(cmd, self.conn)
        re_string = 'quorum\s+majority.*\s*on\s*-\s*no\s*-\s*quorum\s+io\s*-\s*error'
        if result:
            re_result = re_search(self.conn, re_string, result, "bool")
            return re_result

    def primary_drbd(self, resource):
        cmd = f'drbdadm primary {resource}'
        result = utils.exec_cmd(cmd, self.conn)
        if result:
            return True

    def secondary_drbd(self, resource):
        cmd = f'drbdadm secondary {resource}'
        result = utils.exec_cmd(cmd, self.conn)
        if result:
            return True

    def create_node(self, node, ip):
        cmd = f'linstor n c {node} {ip} --node-type Combined'
        result = utils.exec_cmd(cmd, self.conn)
        if result:
            return result

    def create_sp(self, node, sp, lvm_device):
        cmd = f'linstor sp c lvm {node} {sp} {lvm_device}'
        result = utils.exec_cmd(cmd, self.conn)
        if result:
            return result

    def create_rd(self,resource):
        cmd_rd = f'linstor rd c {resource}'
        utils.exec_cmd(cmd_rd, self.conn)

    def creare_vd(self,resource,size):
        cmd_vd = f'linstor vd c {resource} {size}'
        utils.exec_cmd(cmd_vd, self.conn)

    def create_diskful_resource(self, node_list, sp, resource):
        for node in node_list:
            cmd = f'linstor r c {node} {resource} --storage-pool {sp}'
            utils.exec_cmd(cmd, self.conn)

    def create_diskless_resource(self, node, resource):
        cmd = f'linstor r c {node} {resource} --diskless'
        result = utils.exec_cmd(cmd, self.conn)
        if result:
            return result

    def check_resource(self):
        cmd = "linstor r l"
        result = utils.exec_cmd(cmd,self.conn)
        a = re.findall(r'res_quorum', result)
        b = re.findall(r'TieBreaker', result)
        if len(a) == 3 and b == []:
            print("Resource created successfully")
        else:
            print("Resource creation failed")
            sys.exit()

    def delete_resource(self, resource):
        cmd = f'linstor rd d {resource}'
        result = utils.exec_cmd(cmd, self.conn)
        if result:
            return result

    def delete_sp(self, node, sp):
        cmd = f'linstor sp d {node} {sp}'
        result = utils.exec_cmd(cmd, self.conn)
        if result:
            return result

    def delete_node(self, node):
        cmd = f'linstor n d {node}'
        result = utils.exec_cmd(cmd, self.conn)
        if result:
            return result

    def get_device_name(self, resource):
        cmd = f'linstor r lv -r {resource}'
        result = utils.exec_cmd(cmd, self.conn)
        re_string = '/dev/drbd\d+'
        if result:
            re_result = re_search(self.conn, re_string, result, "group")
            return re_result

    def get_linstor_res(self, resource):
        cmd = f'linstor r l -r {resource} -p'
        result = utils.exec_cmd(cmd, self.conn)
        if result:
            return result


class IpService(object):
    def __init__(self, conn=None):
        self.conn = conn

    def down_device(self, device):
        cmd = f"ifconfig {device} down"
        # cmd = f"nmcli device disconnect {device}"
        result = utils.exec_cmd(cmd, self.conn)
        if result:
            return True

    def up_device(self, device):
        # cmd = f"ifconfig {device} up"
        cmd = f"nmcli device connect {device}"
        result = utils.exec_cmd(cmd, self.conn)
        if result:
            return True

class RWData(object):
    def __init__(self, conn=None):
        self.conn = conn

    def dd_operation(self, device):
        cmd = f"dd if=/dev/urandom of={device} oflag=direct status=progress"
        print(self.conn, f"Start dd on {get_global_dict_value(self.conn)}.", 0)
        utils.exec_cmd(cmd, self.conn)

    def get_dd(self):
        cmd = 'ps -ef | grep dd'
        result = utils.exec_cmd(cmd, self.conn)
        if result:
            return result

    def kill_dd(self, pid):
        cmd = f'kill -9 {pid}'
        result = utils.exec_cmd(cmd, self.conn)
        if result:
            return True

class DebugLog(object):
    def __init__(self, conn=None):
        self.conn = conn

    def get_crm_report_file(self, time, path):
        cmd = f'crm_report --from "{time}" {path}/crm_report_${{HOSTNAME}}_$(date +"%Y-%m-%d-%H-%M")_{_TIMES}.log'
        result = utils.exec_cmd(cmd, self.conn)
        if result:
            return True

    def get_dmesg_file(self, path):
        # 显示内核缓冲日志
        cmd = f'dmesg -T | cat > {path}/dmesg_${{HOSTNAME}}_$(date +"%Y-%m-%d-%H-%M")_{_TIMES}.log'
        result = utils.exec_cmd(cmd, self.conn)
        if result:
            return True

    def mkdir_log_dir(self, path):
        cmd = f'mkdir -p {path}'
        result = utils.exec_cmd(cmd, self.conn)
        if result:
            return True

    def rm_log_dir(self, path):
        cmd = f'rm -rf {path}'
        result = utils.exec_cmd(cmd, self.conn)
        if result:
            return True

    def clear_dmesg(self):
        # 清空内核缓存信息
        cmd = f'dmesg -C'
        result = utils.exec_cmd(cmd, self.conn)
        if result:
            return True

    def download_log(self, remote, local):
        if self.conn:
            result = self.conn.download(remote, local)
        else:
            cmd = f'cp -r {remote} {local}'
            result = utils.exec_cmd(cmd)
        if result:
            return True

class InstallSoftware(object):
    def __init__(self, conn=None):
        self.conn = conn

    def update_apt(self):
        """更新apt"""
        cmd = "apt update -y"
        result = utils.exec_cmd(cmd, self.conn)
        if result:
            return True

    def install_spc(self):
        cmd1 = 'apt install -y software-properties-common'
        cmd2 = 'add-apt-repository -y ppa:linbit/linbit-drbd9-stack'
        result1 = utils.exec_cmd(cmd1, self.conn)
        result2 = utils.exec_cmd(cmd2, self.conn)

    def update_pip(self):
        cmd = "python3 -m pip install --upgrade pip"
        result = utils.exec_cmd(cmd, self.conn)
        if result:
            return True

    def install_software(self, name):
        """根据软件名安装对应软件"""
        cmd = f"apt install {name} -y"
        result = utils.exec_cmd(cmd, self.conn)
        if result:
            return True

    def install_drbd(self):
        cmd = 'export DEBIAN_FRONTEND=noninteractive && apt install -y drbd-utils drbd-dkms'
        result = utils.exec_cmd(cmd, self.conn)
        if result:
            return True

    def install_vplx(self):
        func_name = traceback.extract_stack()[-2][2]
        if self.conn:
            result = self.conn.upload("vplx", "/tmp")
        else:
            cmd = f'cp -r "vplx" "/tmp"'
            p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, encoding="utf-8")
            if p.returncode == 0:
                result0 = p.stdout
                result = {"st": True, "rt": result0}
            else:
                result = {"st": False, "rt": p.stderr}
        if result:
            # cmd_pip = f'pip3 install -r /tmp/vplx/requirements.txt'
            # result_pip = utils.exec_cmd(cmd_pip, self.conn)
            # if not result_pip["st"]:
            #     print("Please install python module on /tmp/requirements.txt")
            return True

class QuorumAutoTest(object):
    def __init__(self, config):
        self.config = config    #直接传的解析后的配置文件
        self.conn = Connect(self.config)
        self.vplx_configs = self.config["versaplx"]
        self.node_list = [vplx_config["hostname"] for vplx_config in self.vplx_configs]
        self.skip = False

    def get_sp(self):
        sp = "sp_quorum"
        sp_list = []
        for vplx_config in self.vplx_configs:
            if "sp" in vplx_config.keys():
                sp_list.append(vplx_config["sp"])
        if len(sp_list) == 3 and len(set(sp_list)) == 1:
            self.skip = True
            sp = sp_list[0]
        return sp

    def test_drbd_quorum(self):
        if len(self.conn.list_vplx_ssh) != 3:
            print(f"Please make sure there are three nodes for this test")
        sp = self.get_sp()
        resource = "res_quorum"
        test_times = self.config["test_times"]
        use_case = self.config["use_case"]

        vtel_conn = None
        if None not in self.conn.list_vplx_ssh:
            vtel_conn = self.conn.list_vplx_ssh[0]
        self.clean_dmesg()
        # print(None, f"Start to install software ...", 0)
        # self.install_software()
        # TODO 可优化，使用 LINSTOR API 代码
        install_obj = InstallSoftware(vtel_conn)
        # install_obj.update_pip()
        install_obj.install_vplx()

        self.create_linstor_resource(vtel_conn, sp, resource)

        stor_obj = Stor(vtel_conn)
        print(f"Check DRBD quorum...")
        if not stor_obj.check_drbd_quorum(resource):
            print(f'Abnormal quorum status of {resource}')
            self.get_log()
            self.delete_linstor_resource(vtel_conn, sp, resource)
            print(f"Finished to collect dmesg and exit testing ...")
            sys.exit()
        if not self.cycle_check_drbd_status(resource):
            self.get_log()
            self.delete_linstor_resource(vtel_conn, sp, resource)
            print(f"Finished to collect dmesg and exit testing ...")
            sys.exit()
        device_name = stor_obj.get_device_name(resource)
        device_list = [vplx_config["private_ip"]["device"] for vplx_config in self.vplx_configs]
        if use_case == 1:
            test_conn_list = zip(self.conn.list_vplx_ssh, self.conn.list_vplx_ssh[1:] + self.conn.list_vplx_ssh[:1])
            mode_total_test_times = 3
        if use_case == 2:
            test_conn_list = [(self.conn.list_vplx_ssh[0], self.conn.list_vplx_ssh[1]),
                              (self.conn.list_vplx_ssh[2], self.conn.list_vplx_ssh[1])]
            mode_total_test_times = 2
            device_list.pop(1)
        mode_times = 0
        total_times = mode_total_test_times * test_times
        for conn_list in test_conn_list:
            device = device_list.pop(0)
            node_a = get_global_dict_value(conn_list[0])
            node_b = get_global_dict_value(conn_list[1])
            stor_a = Stor(conn_list[0])
            stor_b = Stor(conn_list[1])
            ip_a = IpService(conn_list[0])
            dd_a = RWData(conn_list[0])
            dd_b = RWData(conn_list[1])
            mode_str = f"\nMode:({node_a}, {node_b}). Mode expect test times: {mode_total_test_times}."
            for i in range(test_times):
                global _TIMES
                times = _TIMES + 1
                _TIMES = times
                print(f"\n{mode_str} test times: {i + 1}. Current test times: {times}. Expect test times: {total_times}.")
                stor_a.primary_drbd(resource)
                print(f"Primary resource on {node_a} ...")
                time.sleep(3)

                thread1 = threading.Thread(target=dd_a.dd_operation,
                                           args=(device_name,), name="thread1")
                thread2 = threading.Thread(target=ip_a.down_device, args=(device,), name="thread2")
                thread3 = threading.Thread(target=dd_b.dd_operation,
                                           args=(device_name,), name="thread3")
                thread4 = threading.Thread(target=stor_a.secondary_drbd, args=(resource,), name="thread4")
                thread1.start()
                time.sleep(20)
                thread2.start()
                print(f"Down {device} on {node_a}  ...")
                thread2.join()
                time.sleep(3)
                stor_b.primary_drbd(resource)
                print(f"Primary resource on {node_b} ...")
                time.sleep(3)
                thread3.start()
                time.sleep(10)
                resource_status_result = stor_a.get_drbd_status(resource)
                re_string = 'quorum:no'
                if resource_status_result:
                    re_result = re_search(conn_list[0], re_string, resource_status_result)

                if re_result:
                    kill_dd(conn_list[0], device_name)
                    if thread1.is_alive():
                        _async_raise(thread1.ident, SystemExit)
                    check_dd(conn_list[0], device_name)
                else:
                    print(f"Configuration 'quorum:no' not exist ...")
                    self.get_log()
                    print(f"Finished to collect dmesg and exit testing ...")
                    sys.exit()
                thread4.start()
                print(f"Secondary resource on {node_a} ...")
                thread4.join()
                thread1.join()
                time.sleep(10)
                kill_dd(conn_list[1], device_name)
                check_dd(conn_list[1], device_name)
                time.sleep(5)
                if thread3.is_alive():
                    _async_raise(thread3.ident, SystemExit)
                    time.sleep(5)
                thread3.join()
                ip_a.up_device(device)
                print(f"Up {device} on {node_a}  ...")
                time.sleep(5)
                if not self.cycle_check_drbd_status(resource):
                    self.get_log()
                    stor_b.secondary_drbd(resource)
                    self.delete_linstor_resource(vtel_conn, sp, resource)
                    print(f"Finished to collect dmesg and exit testing ...")
                    sys.exit()
                stor_b.secondary_drbd(resource)
                print(conn_list[1], f"Secondary resource on {node_b} ...", 0)
                if times == mode_times * test_times + 1:
                    self.get_log()
                    mode_times = mode_times + 1
                print(f"Success. Wait 3 minutes.")
                time.sleep(180)

        self.delete_linstor_resource(vtel_conn, sp, resource)

    def create_linstor_resource(self, conn, sp, resource):
        size = self.config["resource_size"]
        use_case = self.config["use_case"]

        stor_obj = Stor(conn)
        if not self.skip:
            print(f"Start to create node ...")
            for vplx_config in self.vplx_configs:
                stor_obj.create_node(vplx_config["hostname"], vplx_config["private_ip"]["ip"])
            print(f"Start to create storagepool {sp} ...")
            for vplx_config in self.vplx_configs:
                stor_obj.create_sp(vplx_config["hostname"], sp, vplx_config["lvm_device"])
        diskful_node_list = self.node_list[:]
        print(f"Start to create resource {resource} ...")
        if use_case == 1:
            diskless_node = diskful_node_list.pop()
            stor_obj.create_rd(resource)
            stor_obj.creare_vd(resource,size)
            stor_obj.create_diskful_resource(diskful_node_list, sp, resource)
            stor_obj.create_diskless_resource(diskless_node, resource)
            stor_obj.check_resource()
        if use_case == 2:
            stor_obj.create_rd(resource)
            stor_obj.creare_vd(resource,size)
            stor_obj.create_diskful_resource(diskful_node_list, sp, resource)
            stor_obj.check_resource()
        time.sleep(15)

    def delete_linstor_resource(self, conn, sp, resource):
        stor_obj = Stor(conn)
        print(f"Start to delete resource {resource} ...")
        stor_obj.delete_resource(resource)
        time.sleep(3)
        if not self.skip:
            print(f"Start to delete storagepool {sp} ...")
            for node in self.node_list:
                stor_obj.delete_sp(node, sp)
            time.sleep(3)
            print(f"Start to delete node ...")
            for node in self.node_list:
                stor_obj.delete_node(node)

    def get_log(self):
        tmp_path = "/tmp/dmesg"
        lst_get_log = []
        lst_mkdir = []
        lst_download = []
        lst_del_log = []
        log_path = self.config["log_path"]
        print(f"Start to collect dmesg file ...")
        for conn in self.conn.list_vplx_ssh:
            debug_log = DebugLog(conn)
            lst_mkdir.append(gevent.spawn(debug_log.mkdir_log_dir, tmp_path))
            lst_get_log.append(gevent.spawn(debug_log.get_dmesg_file, tmp_path))
            lst_download.append(gevent.spawn(debug_log.download_log, tmp_path, log_path))
            lst_del_log.append(gevent.spawn(debug_log.rm_log_dir, tmp_path))
        gevent.joinall(lst_get_log)
        gevent.joinall(lst_mkdir)
        gevent.joinall(lst_download)
        gevent.joinall(lst_mkdir)
        print(f"Finished to collect dmesg file ...")

    def clean_dmesg(self):
        lst_clean_dmesg = []
        for conn in self.conn.list_vplx_ssh:
            debug_log = DebugLog(conn)
            lst_clean_dmesg.append(gevent.spawn(debug_log.clear_dmesg))
        gevent.joinall(lst_clean_dmesg)

    def check_drbd_status(self, resource):
        resource_status_list = []
        for vplx_conn in self.conn.list_vplx_ssh:
            stor_obj = Stor(vplx_conn)
            resource_status_result = stor_obj.get_drbd_status(resource)
            resource_status = check_drbd_status(vplx_conn, resource_status_result, resource)
            resource_status_list.append(resource_status)
        return resource_status_list

    def cycle_check_drbd_status(self, resource):
        print(f"Check DRBD status...")
        flag = False
        for i in range(100):
            flag = True
            resource_status_list = self.check_drbd_status(resource)
            for resource_status in resource_status_list:
                if resource_status == 'StandAlone':
                    print(f'{time.strftime("%Y/%m/%d %H:%M:%S", time.localtime())} --- Connection is StandAlone')
                    return False
                if resource_status[1] != "UpToDate" and resource_status[1] != "Diskless":
                    status = resource_status[1]
                    time.sleep(180)
                    flag = False
            if flag is True:
                break
        if flag is False:
            print(f'{time.strftime("%Y/%m/%d %H:%M:%S", time.localtime())} --- Resource status: {status}')
        return flag

def main():
    obj_yaml = config_file.ConfFile('../target03.yaml')
    yaml_info_list = obj_yaml.read_yaml()
    test_quorum = QuorumAutoTest(yaml_info_list)
    test_quorum.test_drbd_quorum()

if __name__ == "__main__":
    main()