from utils import exec_command, config_file, network_operator, resources_operator
import time
import utils

class LinstorEviction(object):
    """
    LINSTOR eviction test
    """

    def __init__(self, name):
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
        self.drbd_cmd = resources_operator.DRBD()
        self.linstor_cmd = resources_operator.Linstor()
        self.device_cmd = network_operator.DeviceService()
        self.recover_cmd = f'instor node restore {name}'
        self.restart_cmd = f'systemctl restart linstor-satellite'
        self.sp_cmd = 'linstor controller sp DrbdOptions/AutoEvictAllowEviction false'

    def open_eviction(self):
        self.linstor_cmd.check_resource(self.satellite_node01)
        self.linstor_cmd.check_sp(self.satellite_node01)
        self.linstor_cmd.check_node(self.satellite_node01)
        time.sleep(5)
        #关机
        self.device_cmd.down_device(self.satellite_node02)
        self.linstor_cmd.check_node(self.satellite_node01)
        self.linstor_cmd.check_resource(self.satellite_node01)
        time.sleep(3600)
        self.linstor_cmd.check_node(self.satellite_node01)
        self.linstor_cmd.check_resource(self.satellite_node01)
        time.sleep(2)
        utils.exec_cmd(self.recover_cmd,self.satellite_node01)
        time.sleep(2)
        self.linstor_cmd.check_node(self.satellite_node01)
        self.linstor_cmd.check_resource(self.satellite_node01)
        time.sleep(2)
        utils.exec_cmd(self.restart_cmd,self.satellite_node01)
        time.sleep(2)
        self.linstor_cmd.check_node(self.satellite_node01)
        self.linstor_cmd.check_resource(self.satellite_node01)

    def down_eviction(self):
        utils.exec_cmd(self.sp_cmd, self.satellite_node01)
        time.sleep(2)
        self.linstor_cmd.check_resource(self.satellite_node01)
        self.linstor_cmd.check_sp(self.satellite_node01)
        self.linstor_cmd.check_node(self.satellite_node01)
        time.sleep(5)
        #关机
        self.device_cmd.down_device(self.satellite_node02)
        self.linstor_cmd.check_node(self.satellite_node01)
        self.linstor_cmd.check_resource(self.satellite_node01)
        time.sleep(3600)
        self.linstor_cmd.check_node(self.satellite_node01)
        self.linstor_cmd.check_resource(self.satellite_node01)
        time.sleep(2)
        utils.exec_cmd(self.recover_cmd,self.satellite_node01)
        time.sleep(2)
        self.linstor_cmd.check_node(self.satellite_node01)
        self.linstor_cmd.check_resource(self.satellite_node01)
    def main(self):
        LinstorEviction().open_eviction()
        LinstorEviction().shut_eviction()



