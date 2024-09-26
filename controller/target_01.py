import re
import sys

from utils import exec_command, config_file, network_operator, resources_operator
import time


class QuorumTest(object):
    """
    quorum(q) target01(t01)
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
        self.node0_device = self.yaml_info_list['node'][0]['network_card']
        self.node1_device = self.yaml_info_list['node'][1]['network_card']
        self.node2_device = self.yaml_info_list['node'][2]['network_card']
        self.drbd_cmd = resources_operator.DRBD()
        self.device_cmd = network_operator.DeviceService()

    def quorum_a_loss(self):
        print("测试A开始")
        print("正在关闭A节点的网卡")
        self.device_cmd.disconn_device(self.node0_device, self.controller_node)
        time.sleep(5)
        node_resutl = self.drbd_cmd.drbdadm_status(self.controller_node)
        node01_resutl = self.drbd_cmd.drbdadm_status(self.satellite_node01)
        node02_resutl = self.drbd_cmd.drbdadm_status(self.satellite_node02)
        quorum_node = re.findall('quorum:([\w]*)', str(node_resutl))
        if quorum_node[0] == 'no':
            print('测试通过')
            self.device_cmd.up_device(self.node0_device, self.controller_node)
            time.sleep(5)
        else:
            print('资源状态异常，测试退出')
            sys.exit()

    def quorum_c_loss(self):
        print("测试C开始")
        print("正在关闭C节点的网卡")
        self.device_cmd.disconn_device(self.node1_device, self.satellite_node02)
        time.sleep(5)
        node_resutl = self.drbd_cmd.drbdadm_status(self.controller_node)
        node01_resutl = self.drbd_cmd.drbdadm_status(self.satellite_node01)
        node02_resutl = self.drbd_cmd.drbdadm_status(self.satellite_node02)
        quorum_node02 = re.findall('quorum:([\w]*)', str(node02_resutl))
        if quorum_node02[0] == 'no':
            print('测试通过')
            self.device_cmd.up_device(self.node2_device_device, self.satellite_node02)
            time.sleep(5)
        else:
            print('资源状态异常，测试退出')
            sys.exit()

    def quorum_ab_loss(self):
        print("测试AB开始")
        print("正在关闭A节点的网卡")
        self.device_cmd.disconn_device(self.node0_device, self.controller_node)
        time.sleep(5)
        print("正在关闭B节点的网卡")
        self.device_cmd.disconn_device(self.node1_device, self.satellite_node01)
        time.sleep(5)
        node_resutl = self.drbd_cmd.drbdadm_status(self.controller_node)
        node01_resutl = self.drbd_cmd.drbdadm_status(self.satellite_node01)
        node02_resutl = self.drbd_cmd.drbdadm_status(self.satellite_node02)
        quorum_node = re.findall('quorum:([\w]*)', str(node_resutl))
        quorum_node01 = re.findall('quorum:([\w]*)', str(node01_resutl))
        quorum_node02 = re.findall('quorum:([\w]*)', str(node02_resutl))
        if quorum_node02[0] == 'no' and quorum_node01[0] == 'no' and quorum_node[0] == 'no':
            print("B节点的网卡正在连接")
            self.device_cmd.up_device(self.node1_device, self.satellite_node01)
            time.sleep(5)
            node_resutl = self.drbd_cmd.drbdadm_status(self.controller_node)
            node01_resutl = self.drbd_cmd.drbdadm_status(self.satellite_node01)
            node02_resutl = self.drbd_cmd.drbdadm_status(self.satellite_node02)
            quorum_node = re.findall('quorum:([\w]*)', str(node_resutl))
            quorum_node01 = re.findall('quorum:([\W]*)', str(node01_resutl))
            quorum_node02 = re.findall('quorum:([\W]*)', str(node02_resutl))
            if quorum_node02[0] == 'no' and quorum_node01[0] == 'no' and quorum_node[0] == 'no':
                print("A节点的网卡正在连接")
                self.device_cmd.up_device(self.node0_device, self.controller_node)
                node_resutl = self.drbd_cmd.drbdadm_status(self.controller_node)
                node01_resutl = self.drbd_cmd.drbdadm_status(self.satellite_node01)
                node02_resutl = self.drbd_cmd.drbdadm_status(self.satellite_node02)
                quorum_node = re.findall('disk:([\w]*)', str(node_resutl))
                quorum_node01 = re.findall('disk:([\w]*)', str(node01_resutl))
                quorum_node02 = re.findall('disk:([\w]*)', str(node02_resutl))
                if quorum_node02[0] != 'quorum:no' and quorum_node01[0] != 'quorum:no' and quorum_node[0] != 'quorum:no':
                    print('测试通过')
                else:
                    print('资源状态异常，测试退出')
                    sys.exit()
            else:
                print('资源状态异常，测试退出')
                sys.exit()
        else:
            print('资源状态异常，测试退出')
            sys.exit()

def run():
    quorumtest = QuorumTest()
    quorumtest.quorum_a_loss()
    quorumtest.quorum_c_loss()
    quorumtest.quorum_ab_loss()