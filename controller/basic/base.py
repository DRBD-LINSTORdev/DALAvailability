import yaml
import os
from utils import exec_command, resources_operator


class YamlRead:
    _instance = None

    def __new__(cls, *args, **kw):
        if cls._instance is None:
            cls._instance = object.__new__(cls, *args, **kw)
        return cls._instance

    def __init__(self):
        self.yaml_info = self.yaml_read()

    def yaml_read(self):
        # 获取当前文件的目录
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # 构造config.yaml文件的路径
        config_path = os.path.join(current_dir, '..', '..', 'config.yaml')
        with open(config_path) as f:
            config = yaml.safe_load(f)
        return config

class BaseClass:
    _instance = None

    def __new__(cls, *args, **kw):
        if cls._instance is None:
            cls._instance = object.__new__(cls, *args, **kw)
        return cls._instance

    def __init__(self):
        self.obj_yaml = YamlRead()
        self.yaml_info_list = self.obj_yaml.yaml_info
        self.linstor_cmds = resources_operator.Linstor()
        self.drbd_cmds = resources_operator.DRBD()
        self.lvm_cmds = resources_operator.LVM()
        self.resource_name = self.yaml_info_list['resource_definition']
        self.controller_ip = self.yaml_info_list['controller_ip']
        self.thick_lvm_storagepool_name = self.yaml_info_list['thick_lvm_storagepool_name']
        self.thin_lvm_storagepool_name = self.yaml_info_list['thin_lvm_storagepool_name']
        self.obj_controller = exec_command.SSHconn(host=self.yaml_info_list['node'][0]['ip']
                                                   , name=self.yaml_info_list['node'][0]['name']
                                                   , username=self.yaml_info_list['node'][0]['username']
                                                   , password=self.yaml_info_list['node'][0]['password'])
        self.obj_satellite01 = exec_command.SSHconn(host=self.yaml_info_list['node'][1]['ip']
                                                    , name=self.yaml_info_list['node'][1]['name']
                                                    , username=self.yaml_info_list['node'][1]['username']
                                                    , password=self.yaml_info_list['node'][1]['password'])
        self.obj_satellite02 = exec_command.SSHconn(host=self.yaml_info_list['node'][2]['ip']
                                                    , name=self.yaml_info_list['node'][2]['name']
                                                    , username=self.yaml_info_list['node'][2]['username']
                                                    , password=self.yaml_info_list['node'][2]['password'])
        self.obj_list = []
        self.nodename_list = []
        self.bmc_list = []
        self._init_obj_list()
        self._init_nodename_list()
        self._init_bmc_list()

    def _init_nodename_list(self):
        self.nodename_list.append(self.yaml_info_list['node'][0]['name'])
        self.nodename_list.append(self.yaml_info_list['node'][1]['name'])
        self.nodename_list.append(self.yaml_info_list['node'][2]['name'])


    def _init_obj_list(self):
        self.obj_list.append(self.obj_controller)
        self.obj_list.append(self.obj_satellite01)
        self.obj_list.append(self.obj_satellite02)

    def _init_bmc_list(self):
        self.bmc_list.append(self.yaml_info_list['node'][0]['bmc'])
        self.bmc_list.append(self.yaml_info_list['node'][1]['bmc'])
        self.bmc_list.append(self.yaml_info_list['node'][2]['bmc'])


