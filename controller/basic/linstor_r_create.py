from .base import BaseClass
from utils import utils
import re
import sys


class MainOperation(BaseClass):
    """
    前提条件：
        确保每个节点有且仅有一个存储池
    """
    def __init__(self):
        super().__init__()
        self.data_list = []
        self._init_datalist()

    def _calculate_r_size(self, info, nodename):
        try:
            result1 = re.findall(r'\|\s+(\S+)\s+\|\s+%s' % nodename, info)
            result1_1 = re.findall(r'%s\s+\|\s+\S+\s+\|\s+\S+\s+\|\s+(\S+)\s+(\S+)\s+\|' % nodename, info)
            sp_name = result1[-1]
            r_size = int(float(result1_1[1][0])) // 2 + 1
            r_size_1 = str(r_size) + result1_1[1][1]
            return sp_name, r_size_1
        except Exception as r :
            print(f"计算创建resource容量错误\n{r}")
            sys.exit()

    def _init_datalist(self):
        sp_info = utils.exec_cmd(f"linstor sp l",self.obj_list[0])

        for i in range(len(self.obj_list)):
            data = self._calculate_r_size(sp_info, self.nodename_list[i])
            self.data_list.append(data)


    def create_rd_vd(self):
        print(f"创建rd和vd;vd的容量为:{self.data_list[0][1]}")
        utils.exec_cmd(f"linstor rd c {self.resource_name}",self.obj_list[0])
        utils.exec_cmd(f"linstor vd c {self.resource_name} {self.data_list[0][1]}",self.obj_list[0])

    def create_2diskful(self):
        print(f"在节点{self.nodename_list[0]}和节点{self.nodename_list[1]}上创建2diskful")
        for i in range(2):
            utils.exec_cmd(f"linstor r c {self.nodename_list[i]} {self.resource_name} --storage-pool {self.data_list[i][0]}",self.obj_list[i])
        utils.exec_cmd(f"linstor r d {self.nodename_list[-1]} {self.resource_name}",self.obj_list[-1])

    def create_2diskful1diskless(self):
        print(f"在2diskful的基础上创建1diskless,即在节点{self.nodename_list[-1]}上创建diskless")
        utils.exec_cmd(f"linstor r c {self.nodename_list[-1]} {self.resource_name} --diskless",self.obj_list[-1])

    def create_3diskful(self):
        print(f"在2diskful+1diskless的基础上创建3diskful,删除节点{self.nodename_list[-1]}的diskless并重新创建为diskful")
        utils.exec_cmd(f"linstor r d {self.nodename_list[-1]} {self.resource_name}",self.obj_list[-1])
        utils.exec_cmd(f"linstor r c {self.nodename_list[-1]} {self.resource_name} --storage-pool {self.data_list[2][0]}",self.obj_list[-1])

    def create_auto_2diskful(self):
        print("在3diskful的基础上自动放置2diskful,删除所有节点上的diskful,使用自动放置2diskful")
        for i in range(len(self.obj_list)):
                utils.exec_cmd(f"linstor r d {self.nodename_list[i]} {self.resource_name}",self.obj_list[i])
        utils.exec_cmd(f"linstor r c {self.resource_name} --auto-place 2",self.obj_list[0])

    def _clear(self):
        print("清理环境")
        utils.exec_cmd(f"linstor rd d {self.resource_name}",self.obj_list[0])


def main():
    print("------------测试开始：资源创建------------")
    print("计算需要创建资源的容量大小")
    Test = MainOperation()
    Test.create_rd_vd()
    Test.create_2diskful()
    Test.create_2diskful1diskless()
    Test.create_3diskful()
    Test.create_auto_2diskful()
    Test._clear()
    print("------------测试结束：资源创建------------")

if __name__ == "__main__":
    main()