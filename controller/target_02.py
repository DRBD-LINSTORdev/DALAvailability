import sys
import time
from threading import Thread
from .. import utils
from utils import exec_command, config_file, network_operator, resources_operator
import timeout_decorator
import re

class DdOperation:
    def __init__(self,obj_ssh,resource_name,device_name,write_of='./test.txt'):
        self.linstor_cmds = resources_operator.Linstor()
        self.drbd_cmds = resources_operator.DRBD()
        self.lvm_cmds = resources_operator.LVM()
        self.obj_ssh = obj_ssh
        self.r_name = resource_name
        self.obj_dd = utils.RWData()
        self.write_of = write_of
        self.device_name = device_name

    def get_devicename(self):
        check_cmd = self.linstor_cmds.check_resource_detailed(self.r_name)
        info = utils.exec_cmd(check_cmd,self.obj_ssh)
        data = re.findall(f'\|([\w\s\/]+)', info)
        data1 = data[5]
        data2 = data1.strip()
        print(f"devicename已获取，为{data2}")
        return data2

    def use_dd_to_write_data(self): #用多线程重写
        try:
            devicename = self.device_name
            print("................................线程1:开始执行dd写数据操作")
            dd_write_cmd = self.obj_dd.dd_write(devicename)
            utils.exec_cmd(dd_write_cmd, self.obj_ssh)
            time.sleep(5)
            print(".....................dd写数据操作执行完毕,dd进程已被关闭")
        except:
            print("dd写数据操作执行失败")
            sys.exit()

    def user_dd_to_read_data(self):
        try:
            devicename = self.device_name
            print("................................线程1:开始执行dd读数据操作")
            dd_read_cmd = self.obj_dd.dd_read(devicename,self.write_of)
            utils.exec_cmd(dd_read_cmd, self.obj_ssh)
            time.sleep(5)
            print(".....................dd读数据操作执行完毕,dd进程已被关闭")
        except:
            print("dd读数据操作执行失败")
            sys.exit()

    def start_dd_write(self):
        state1 = Thread(target=self.use_dd_to_write_data)
        state1.setDaemon(True)
        state1.start()

    def start_dd_read(self):
        state1 = Thread(target=self.user_dd_to_read_data())
        state1.setDaemon(True)
        state1.start()

    def stop_dd(self):
        try:
            process_info = utils.exec_cmd('ps -A | grep dd',self.obj_ssh)
            result01 = re.findall(f'([\d]+) \?        \w\w:\w\w:\w\w dd',process_info)
            r_dd_pid = result01[0]
            kill_dd_cmd = self.obj_dd.kill_dd(r_dd_pid)
            utils.exec_cmd(kill_dd_cmd,self.obj_ssh)
            print("dd进程已终止")
        except:
            print("停止dd写数据出现错误")
            sys.exit()

class MainOperation:
    def __init__(self):
        self.obj_yaml = config_file.ConfFile('./config.yaml')
        self.yaml_info_list = self.obj_yaml.read_yaml()
        self.linstor_cmds = resources_operator.Linstor()
        self.drbd_cmds = resources_operator.DRBD()
        self.lvm_cmds = resources_operator.LVM()
        self.network_cmds = network_operator.DeviceService()
        self.scp_cmds = network_operator.Scp()
        self.obj_controller = exec_command.SSHconn(host=self.yaml_info_list['node'][0]['ip']
                                                   , username=self.yaml_info_list['node'][0]['username']
                                                   , password=self.yaml_info_list['node'][0]['password'])
        self.obj_satellite01 = exec_command.SSHconn(host=self.yaml_info_list['node'][1]['ip']
                                                    , username=self.yaml_info_list['node'][1]['username']
                                                    , password=self.yaml_info_list['node'][1]['password'])
        self.obj_satellite02 = exec_command.SSHconn(host=self.yaml_info_list['node'][2]['ip']
                                                    , username=self.yaml_info_list['node'][2]['username']
                                                    , password=self.yaml_info_list['node'][2]['password'])

    def get_devicename(self):
        info = utils.exec_cmd("linstor r lv | grep resourcetest01",self.obj_controller) #资源名？
        data = re.findall(f'\|([\w\s\/]+)', info)
        data1 = data[5]
        data2 = data1.strip()
        print(f"devicename已获取，为{data2}")
        return data2

    def thread_operation(self,func_name):
        status = Thread(target=func_name)
        status.setDaemon(True)
        status.start()

    @timeout_decorator.timeout(80)
    def user_go_meter_to_write(self,ssh_obj):
        go_meter_write_cmd = "./main write"
        utils.exec_cmd(go_meter_write_cmd,ssh_obj)

    def user_go_meter_to_compare(self,ssh_obj):
        go_meter_compare_cmd = "./main compare"
        utils.exec_cmd(go_meter_compare_cmd,ssh_obj)

    def step1(self):
        try:
            self.thread_operation(self.user_go_meter_to_write(self.obj_controller))
        except:
            print("go-meter未自动成功退出")
            sys.exit()

        time.sleep(25)
        disconnect_network = self.network_cmds.down_device(self.yaml_info_list['node'][0]['bond_ip'],self.obj_controller)
        time.sleep(55)
        a_to_b_file = self.scp_cmds.local_to_remote_file('/root/record.txt'
                                                             ,self.yaml_info_list['node'][1]['username']
                                                             ,self.yaml_info_list['node'][1]['password']
                                                             ,'/root/record.txt'
                                                             ,self.obj_controller)
        self.user_go_meter_to_compare(self.obj_controller)  #如何判断运行失败
        self.user_go_meter_to_write(self.obj_controller)    #如何判断运行失败
        self.user_go_meter_to_compare(self.obj_satellite01)
        connect_network = self.network_cmds.up_device(self.yaml_info_list['node'][0]['bond_ip'],self.obj_controller)

    def step2(self):
        try:
            self.thread_operation(self.user_go_meter_to_write(self.obj_satellite02))
        except:
            print("go-meter未自动成功退出")
            sys.exit()

        time.sleep(25)
        disconnect_network = self.network_cmds.down_device(self.yaml_info_list['node'][2]['bond_ip'],self.obj_satellite02)
        time.sleep(55)
        c_to_a_file = self.scp_cmds.local_to_remote_file('/root/record.txt'
                                                             , self.yaml_info_list['node'][0]['username']
                                                             , self.yaml_info_list['node'][0]['password']
                                                             , '/root/record.txt'
                                                             ,self.obj_satellite02)
        c_to_b_file = self.scp_cmds.local_to_remote_file('/root/record.txt'
                                                             , self.yaml_info_list['node'][1]['username']
                                                             , self.yaml_info_list['node'][1]['password']
                                                             , '/root/record.txt'
                                                             ,self.obj_satellite02)
        self.user_go_meter_to_compare(self.obj_satellite02)
        self.user_go_meter_to_write(self.obj_satellite02)
        self.user_go_meter_to_compare(self.obj_controller)
        self.user_go_meter_to_compare(self.obj_satellite01)
        connect_network = self.network_cmds.up_device(self.yaml_info_list['node'][2]['bond_ip'],self.obj_satellite02)

    def step3(self):
        device_name = self.get_devicename()
        mkfs_cmd = f'mkfs.ext4 {device_name}'
        mount_cmd = f'mount {device_name} /mnt'
        umount_cmd = f'umount -v {device_name}'
        check_mount_cmd = "df -h"

        utils.exec_cmd(mkfs_cmd,self.obj_controller)
        utils.exec_cmd(mount_cmd, self.obj_controller)
        disconnect_network = self.network_cmds.down_device(self.yaml_info_list['node'][0]['bond_ip'],self.obj_controller)
        result01 = utils.exec_cmd(check_mount_cmd, self.obj_controller)
        try:
            result02 = re.findall(r'\n(%s)[\w\W]*(/mnt)' % device_name, str(result01))
            if result02[0] == ('/dev/drbd1000', '/mnt'):
                print("df -h状态正常")
        except:
            print("df -h状态错误")
            sys.exit()

        obj_dd = DdOperation(self.obj_controller,"resourcetest01",device_name="/mnt/test.txt")
        obj_dd.start_dd_write()
        obj_dd.start_dd_read()
        utils.exec_cmd(umount_cmd,self.obj_controller)
        utils.exec_cmd(mount_cmd,self.obj_satellite01)
        utils.exec_cmd(umount_cmd, self.obj_satellite01)
        connect_network = self.network_cmds.up_device(self.yaml_info_list['node'][0]['bond_ip'],self.obj_controller)

    def step4(self):
        device_name = self.get_devicename()
        mkfs_cmd = f'mkfs.ext4 {device_name}'
        mount_cmd = f'mount {device_name} /mnt'
        umount_cmd = f'umount -v {device_name}'
        check_mount_cmd = "df -h"

        utils.exec_cmd(mkfs_cmd,self.obj_satellite02)
        utils.exec_cmd(mount_cmd, self.obj_satellite02)
        disconnect_network = self.network_cmds.down_device(self.yaml_info_list['node'][2]['bond_ip'],self.obj_satellite02)
        result01 = utils.exec_cmd(check_mount_cmd, self.obj_satellite02)
        try:
            result02 = re.findall(r'\n(%s)[\w\W]*(/mnt)' % device_name, str(result01))
            if result02[0] == ('/dev/drbd1000', '/mnt'):
                print("df -h状态正常")
        except:
            print("df -h状态错误")
            sys.exit()

        obj_dd = DdOperation(self.obj_controller,"resourcetest01",device_name="/mnt/test.txt")
        obj_dd.start_dd_write()
        obj_dd.start_dd_read()
        utils.exec_cmd(umount_cmd, self.obj_satellite02)
        utils.exec_cmd(mount_cmd,self.obj_controller)
        utils.exec_cmd(umount_cmd, self.obj_controller)
        utils.exec_cmd(mount_cmd,self.obj_satellite01)
        utils.exec_cmd(umount_cmd, self.obj_satellite01)
        connect_network = self.network_cmds.up_device(self.yaml_info_list['node'][2]['bond_ip'],self.obj_satellite02)

def main():
    test_obj = MainOperation()
    test_obj.step1()
    test_obj.step2()
    test_obj.step3()
    test_obj.step4()