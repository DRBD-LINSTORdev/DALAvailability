from utils import exec_command, config_file, resources_operator
import time
import re


class SinglePrimary(object):
    """
    Single Primary test
    """

    def __int__(self):
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


    def diskful_primary(self):

        self.drbd_cmd.drbdadm_priamry('resourcetest01', self.controller_node)

        node01_result = self.drbd_cmd.drbdadm_priamry('resourcetest01', self.satellite_node01)
        nodeo1_command = re.findall('Command\s*(.*)', node01_result)
        if nodeo1_command:
            print(nodeo1_command)
            node02_result = self.drbd_cmd.drbdadm_priamry(self.satellite_node02)
            nodeo2_command = re.findall('Command\s*(.*)', node02_result)
            if nodeo2_command:
                print(nodeo2_command)
                self.drbd_cmd.drbdadm_status(self.controller_node)
                time.sleep(2)
                self.linstor_cmd.check_resource(self.controller_node)

    def diskless_primary(self):
        self.drbd_cmd.drbdadm_secondary(self.controller_node)
        self.drbd_cmd.drbdadm_priamry(self.satellite_node02)
        node01_result = self.drbd_cmd.drbdadm_priamry(self.satellite_node01)
        nodeo1_command = re.findall('Command\s*(.*)', node01_result)
        if nodeo1_command:
            print(nodeo1_command)
            node_result = self.drbd_cmd.drbdadm_priamry(self.controller_node)
            node_command = re.findall('Command\s*(.*)', node_result)
            if node_command:
                print(node_command)
                self.drbd_cmd.drbdadm_status(self.controller_node)
                time.sleep(2)
                self.linstor_cmd.check_resource(self.controller_node)

    def main(self):
        SinglePrimary().diskful_primary()
        SinglePrimary().diskless_primary()
