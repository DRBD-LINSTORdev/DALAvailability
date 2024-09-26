import sys
from .base import BaseClass
from utils import utils
import re


class MainOperation(BaseClass):
    """
    """
    def __init__(self):
        super().__init__()
        self.vgname_info = []
        self.size_info = []
        self._init_data()

    def _init_data(self):
        vg_info = []

        for i in range(len(self.obj_list)):
            vg_info.append(utils.exec_cmd("vgs",self.obj_list[i]))
            self.vgname_info.append(self.yaml_info_list['node'][i]['thinpool_vgname'])

        for i,z in zip(vg_info,self.vgname_info):
            self.size_info.append(self._calculate_size(i,z))

    def _calculate_size(self, info, vgname):
        try:
            result1 = re.findall(r'%s\s+\d+\s+\d+\s+\d+\s+\S+\s+(\S+)' % vgname, info)
            result2 = re.findall(r'(\d+)', result1[0])
            result2_1 = re.findall(r'([a-z]+)', result1[0])
            result3 = int(result2[0]) // 2
            result4 = str(result3) + result2_1[0]
            return result4
        except:
            print("计算创建thinpool容量错误")
            sys.exit()

    def create_thinpool(self):
        for i in range(len(self.obj_list)):
            print(f"在节点:{self.obj_list[i]._name} 创建名为 pooltest 的thinpool,大小为{self.size_info[i]}")
            utils.exec_cmd(f"lvcreate -L {self.size_info[i]} --thinpool pooltest {self.vgname_info[i]}",self.obj_list[i])

    def create_sp(self):
        for i in range(len(self.obj_list)):
            print(f"在节点:{self.obj_list[i]._name} 创建名为 {self.thin_lvm_storagepool_name} 的thin lvm存储池")
            thinpoolname = self.vgname_info[i] + "/" + "pooltest"
            utils.exec_cmd(
                f"linstor sp c lvmthin {self.nodename_list[i]} {self.thin_lvm_storagepool_name} {thinpoolname}",
                self.obj_list[i])


def main():
    print("------------测试开始：thin lvm 存储池创建------------")
    print("计算各个节点创建thinpool的容量大小")
    Test = MainOperation()
    Test.create_thinpool()
    Test.create_sp()
    print("------------测试结束：thin lvm 存储池创建------------")


if __name__ == "__main__":
    main()
