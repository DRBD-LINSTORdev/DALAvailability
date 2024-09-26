from utils import exec_command, config_file, resources_operator
import utils
import re
import sys
import time
from threading import Thread


class AutomaticPromotion(object):
    """
     Automatic Promotion test
    """

    def __init__(self):
        self.yaml_file = config_file.ConfFile('../config.yaml')
        self.yaml_info_list = self.yaml_file.read_yaml()
        self.controller_node = exec_command.SSHconn(host=self.yaml_info_list['node'][0]['ip']
                                                    , username=self.yaml_info_list['node'][0]['username']
                                                    , password=self.yaml_info_list['node'][0]['password'])
        self.satellite_node01 = exec_command.SSHconn(host=self.yaml_info_list['node'][1]['ip']
                                                     , username=self.yaml_info_list['node'][1]['username']
                                                     , password=self.yaml_info_list['node'][1]['password'])
        self.satellite_node02 = exec_command.SSHconn(host=self.yaml_info_list['node'][2]['ip']
                                                     , username=self.yaml_info_list['node'][2]['username']
                                                     , password=self.yaml_info_list['node'][2]['password'])
        self.disk_cmd = resources_operator.DISK()
        self.drbd_cmd = resources_operator.DRBD()
        self.dd_cmds = utils.RWData()
        check_cmd = resources_operator.Linstor().check_resource_detailed('resourcetest01', self.controller_node)
        result = re.findall(f'\|([\w\s\/]+)', check_cmd)
        self.device_name = result[5].strip()

    def use_dd_to_write_data(self, obj_ssh):
        try:
            print("................................线程1:开始执行dd写数据操作")
            dd_write = self.dd_cmds.dd_write(self.device_name, obj_ssh)
            time.sleep(5)
            print(".....................dd写数据操作执行完毕,dd进程已被关闭")
        except:
            print("dd写数据操作执行失败")
            sys.exit()

    def start_dd_write(self,obj_ssh):
        state = Thread(target=self.use_dd_to_write_data(obj_ssh=obj_ssh))
        state.setDaemon(True)
        state.start()

    def kill_dd(self, obj_ssh):
        try:
            process_info = utils.exec_cmd('ps -A | grep dd', obj_ssh)
            result01 = re.findall(f'(\S+)\s+\S+\s+\S+\s+dd', process_info)
            r_dd_pid = result01[0]
            kill_dd_cmd = self.dd_cmds.kill_dd(r_dd_pid)
            utils.exec_cmd(kill_dd_cmd, obj_ssh)
            print("dd进程已终止")
        except:
            print("停止dd写数据出现错误")
            sys.exit()

    def diskful_mount(self):
        print("正在格式化磁盘，请稍等")
        result = self.disk_cmd.mkfs_disk(self.device_name,self.controller_node)
        node_information = re.findall('information: ([\w]*)',str(result))
        if node_information[0] == 'done':
            print('磁盘格式化完成')
        else:
            print('磁盘格式化失败，测试退出')
            sys.exit()
        print("diskful_mount测试开始")
        try:
            self.disk_cmd.mount_disk(self.device_name, self.controller_node)
            node_result = self.drbd_cmd.drbdadm_status(self.controller_node)
            node_role = re.findall('role:([\w]*)', str(node_result))
            if node_role[0] == 'Primary' and node_role[1] == 'Secondary' and node_role[2] == 'Secondary':
                print('节点状态正常')
                time.sleep(2)
                self.disk_cmd.umount_disk(self.device_name, self.controller_node)
                node_result = self.drbd_cmd.drbdadm_status(self.controller_node)
                node_role = re.findall('role:([\w]*)', str(node_result))
                if node_role[0] == 'Secondary':
                    print("diskful_mount测试通过")
            else:
                print("节点状态异常,diskful_mount测试不通过")
        except Exception as e:
            print(e)
            print("磁盘挂载失败，测试退出")
            sys.exit()

    def diskless_mount(self):
        print("diskless_mount测试开始")
        try:
            self.disk_cmd.mount_disk(self.device_name, self.satellite_node02)
            node_result = self.drbd_cmd.drbdadm_status(self.controller_node)
            node_role = re.findall('role:([\w]*)', str(node_result))
            if node_role[0] == 'Secondary' and node_role[1] == 'Secondary' and node_role[2] == 'Primary':
                    print('节点状态正常')
                    time.sleep(2)
                    self.disk_cmd.umount_disk(self.device_name, self.satellite_node02)
                    node_result = self.drbd_cmd.drbdadm_status(self.controller_node)
                    node_role = re.findall('role:([\w]*)', str(node_result))
                    if node_role[2] == 'Secondary':
                        print("diskless_mount测试通过")
            else:
                print("节点状态异常,diskless_mount测试不通过")
        except Exception as e:
            print(e)
            print("磁盘挂载失败，测试退出")
            sys.exit()

    def diskful_dd(self):
        print("diskful_dd测试开始")
        self.start_dd_write(self.controller_node)
        node_result = self.drbd_cmd.drbdadm_status(self.controller_node)
        node_role = re.findall('role:([\w]*)', str(node_result))
        if node_role[0] == 'Primary' and node_role[1] == 'Secondary' and node_role[2] == 'Secondary':
                self.kill_dd(self.controller_node)
                node_result = self.drbd_cmd.drbdadm_status(self.controller_node)
                node_role = re.findall('role:([\w]*)', str(node_result))
                if node_role[0] == 'Secondary':
                    print("节点状态正常，测试通过")
        else:
            print("节点状态异常，测试退出")

    def diskless_dd(self):
        print("diskless_dd测试开始")
        self.start_dd_write(self.satellite_node02)
        node_result = self.drbd_cmd.drbdadm_status(self.controller_node)
        node_role = re.findall('role:([\w]*)', str(node_result))
        if node_role[0] == 'Secondary' and node_role[1] == 'Secondary' and node_role[2] == 'Primary':
                self.kill_dd(self.satellite_node02)
                node_result = self.drbd_cmd.drbdadm_status(self.controller_node)
                node_role = re.findall('role:([\w]*)', str(node_result))
                if node_role[2] == 'Secondary':
                    print("节点状态正常，测试通过")
        else:
            print("节点状态异常，测试退出")


def run():
    test_automatic_promotion = AutomaticPromotion()
    test_automatic_promotion.diskful_mount()
    test_automatic_promotion.diskless_mount()
    test_automatic_promotion.diskful_dd()
    test_automatic_promotion.diskless_dd()
