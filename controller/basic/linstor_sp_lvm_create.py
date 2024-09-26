from .base import BaseClass
from utils import utils


class MainOperation(BaseClass):
    def __init__(self):
        super().__init__()


    def create_sp(self):
        print(f"在节点:{self.nodename_list[0]}创建thick lvm 存储池: {self.thick_lvm_storagepool_name}")
        utils.exec_cmd(f"linstor sp c lvm {self.yaml_info_list['node'][0]['name']} {self.thick_lvm_storagepool_name} {self.yaml_info_list['node'][0]['vgname']}",self.obj_controller)
        print(f"在节点:{self.nodename_list[1]}创建thick lvm 存储池: {self.thick_lvm_storagepool_name}")
        utils.exec_cmd(f"linstor sp c lvm {self.yaml_info_list['node'][1]['name']} {self.thick_lvm_storagepool_name} {self.yaml_info_list['node'][1]['vgname']}",self.obj_satellite01)
        print(f"在节点:{self.nodename_list[2]}创建thick lvm 存储池: {self.thick_lvm_storagepool_name}")
        utils.exec_cmd(f"linstor sp c lvm {self.yaml_info_list['node'][2]['name']} {self.thick_lvm_storagepool_name} {self.yaml_info_list['node'][2]['vgname']}",self.obj_satellite02)


def main():
    Test = MainOperation()
    print("------------测试开始：thick lvm 存储池创建------------")
    Test.create_sp()
    print("------------测试结束：thick lvm 存储池创建------------")


if __name__ == "__main__":
    main()