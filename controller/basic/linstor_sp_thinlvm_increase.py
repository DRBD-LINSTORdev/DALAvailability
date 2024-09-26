import sys
import re
from .base import BaseClass
from utils import utils


class MainOperation(BaseClass):
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
            result1 = re.findall(r'%s\s+\d+\s+\d+\s+\d+\s+\S+\s+\S+\s+(\S+)' % vgname, info)
            result2 = re.findall(r'(\d+)', result1[0])
            result2_1 = re.findall(r'([a-z]+)', result1[0])
            result3 = int(result2[0]) // 2
            if result3 == 0:
                print("vg剩余空间不足，无法扩容")
            result4 = str(result3) + result2_1[0]
            return result4
        except:
            print("计算创建thinpool容量错误")
            sys.exit()

    def increase_thinpool(self):
        for i in range(len(self.obj_list)):
            print(f"在节点:{self.obj_list[i]._name}进行thin lvm存储池扩容，增加容量:{self.size_info[i]}")
            thinpoolname = self.vgname_info[i] + "/" + "pooltest"
            utils.exec_cmd(f"lvresize -L +{self.size_info[i]} {thinpoolname}",self.obj_list[i])


def main():
    print("------------测试开始：thin lvm 存储池扩容------------")
    print("计算各个节点扩容thinpool的大小")
    Test = MainOperation()
    Test.increase_thinpool()
    print("------------测试结束：thin lvm 存储池扩容------------")


if __name__ == "__main__":
    main()