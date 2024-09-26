import yaml
from utils import exec_command, resources_operator


class YamlRead:
    def __init__(self):
        self.yaml_info = self.yaml_read()

    def yaml_read(self):
        with open('../config.yaml') as f:
            config = yaml.safe_load(f)
        return config

class MainOperation:
    def __init__(self):
        self.obj_yaml = YamlRead()
        self.yaml_info_list = self.obj_yaml.yaml_info
        self.linstor_cmds = resources_operator.Linstor()
        self.drbd_cmds = resources_operator.DRBD()
        self.lvm_cmds = resources_operator.LVM()
        self.resource_name = self.yaml_info_list['resource_definition']
        self.obj_controller = exec_command.SSHconn(host=self.yaml_info_list['node'][0]['ip']
                                                   , username=self.yaml_info_list['node'][0]['username']
                                                   , password=self.yaml_info_list['node'][0]['password'])
        self.obj_satellite01 = exec_command.SSHconn(host=self.yaml_info_list['node'][1]['ip']
                                                    , username=self.yaml_info_list['node'][1]['username']
                                                    , password=self.yaml_info_list['node'][1]['password'])
        self.obj_satellite02 = exec_command.SSHconn(host=self.yaml_info_list['node'][2]['ip']
                                                    , username=self.yaml_info_list['node'][2]['username']
                                                    , password=self.yaml_info_list['node'][2]['password'])
