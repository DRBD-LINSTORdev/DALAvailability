from .base import BaseClass
from utils import utils


class MainOperation(BaseClass):
    def __init__(self):
        super().__init__()

    def create_node(self):
        print(f"创建Controller节点{self.nodename_list[0]}")
        utils.exec_cmd(f"linstor node create {self.yaml_info_list['node'][0]['name']} {self.yaml_info_list['node'][0]['ip']} --node-type Controller",self.obj_controller)
        print(f"创建Combined节点{self.nodename_list[1]}")
        utils.exec_cmd(f"linstor node create {self.yaml_info_list['node'][1]['name']} {self.yaml_info_list['node'][1]['ip']} --node-type Combined",self.obj_satellite01)
        print(f"创建Satellite节点{self.nodename_list[2]}")
        utils.exec_cmd(f"linstor node create {self.yaml_info_list['node'][2]['name']} {self.yaml_info_list['node'][2]['ip']} --node-type Satellite",self.obj_satellite02)


def main():
    Test = MainOperation()
    print("------------测试开始：linstor节点创建------------")
    Test.create_node()
    print("------------测试结束：linstor节点创建------------")


if __name__ == "__main__":
    main()