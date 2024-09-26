import sys
import re
from .base import BaseClass
from utils import utils


class MainOperation(BaseClass):
    """
    前提条件：
        确保每个节点有且仅有一个存储池
    """
    def __init__(self):
        super().__init__()
        self.data_list = []
        self._init_datalist()


    def _init_datalist(self):
        vd_info = utils.exec_cmd(f"linstor vd l",self.obj_list[0])
        vd_result01 = re.findall(f'%s\s+\|\s+(\S+)\s+\|\s+\S+\s+\|\s+(\S+)\s+(\S+)'%self.resource_name, vd_info)

        size = int(vd_result01[0][1]) // 2 + 1
        size_1 = str(size) + vd_result01[0][-1]

        self.data_list.append(vd_result01[0][0])
        self.data_list.append(size_1)

        return self.data_list



    def increase_resource(self):
        print(f"对资源:{self.resource_name}进行扩缩减，缩减后的大小为:{self.data_list[-1]}")
        utils.exec_cmd(f"linstor vd set-size {self.resource_name} {self.data_list[0]} {self.data_list[-1]}",self.obj_list[0])


def main():
    print("------------测试开始：资源缩减------------")
    print("计算资源需要缩减的大小")
    Test = MainOperation()
    Test.increase_resource()
    print("------------测试开始：资源缩减------------")


if __name__ == "__main__":
    main()