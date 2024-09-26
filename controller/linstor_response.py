from utils import exec_command, config_file
import utils
import re


class LinstorCommandResponse(object):
    """
     LINSTOR响应测试
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

    def response_test(self):
        cmd = f'time linstor n l'
        node_result = utils.exec_cmd(cmd,self.controller_node)
        node01_result = utils.exec_cmd(cmd, self.satellite_node01)
        node02_result = utils.exec_cmd(cmd, self.satellite_node02)

        real = str(re.findall('real(.*?)user', node_result)).strip()
        user = str(re.findall('user(.*?)sys', node_result)).strip()
        sys = str(re.findall('sys(.*?)', node_result)).strip()

        real01 = str(re.findall('real(.*?)user', node01_result)).strip()
        user01 = str(re.findall('user(.*?)sys', node01_result)).strip()
        sys01 = str(re.findall('sys(.*?)', node01_result)).strip()

        real02 = str(re.findall('real(.*?)user', node02_result)).strip()
        user02 = str(re.findall('user(.*?)sys', node02_result)).strip()
        sys02 = str(re.findall('sys(.*?)', node02_result)).strip()

        utils.Table('node_name', 'real','user','sys')
        utils.Table().add_row(['node', real, user, sys])
        utils.Table().add_row(['node01', real01, user01, sys01])
        utils.Table().add_row(['node02', real02, user02, sys02])
        utils.Table().print_table()

def main():
    LinstorCommandResponse().response_test()
