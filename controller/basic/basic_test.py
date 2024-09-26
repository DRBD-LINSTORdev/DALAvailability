from .base import BaseClass
from utils import utils
from . import drbd_status
from . import linstor_node_create
from . import linstor_node_delete
from . import linstor_sp_thinlvm_create
from . import linstor_sp_thinlvm_increase
from . import linstor_sp_thinlvm_delete
from . import linstor_sp_lvm_create
from . import linstor_sp_lvm_delete
from . import linstor_r_create
from . import linstor_r_increase
from . import linstor_r_decrease
from . import linstor_r_promotion
from . import linstor_r_eviction
from . import linstor_report_error
from . import linstor_report_sos
from . import linstor_responsetime
from . import trim
import re
import sys




class MainOperation(BaseClass):
    """
    前提条件：

    """
    def __init__(self):
        super().__init__()
        self.data_list = []


    def _init_datalist(self):
        sp_info = utils.exec_cmd(f"linstor sp l",self.obj_list[0])

        for i in range(len(self.obj_list)):
            data = self._calculate_r_size(sp_info, self.nodename_list[i])
            self.data_list.append(data)

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

    def linstor_node_create_s(self):
        print(f"创建Combined节点{self.nodename_list[0]}")
        utils.exec_cmd(f"linstor node create {self.yaml_info_list['node'][0]['name']} {self.yaml_info_list['node'][0]['ip']} --node-type Combined",self.obj_controller)
        print(f"创建Combined节点{self.nodename_list[1]}")
        utils.exec_cmd(f"linstor node create {self.yaml_info_list['node'][1]['name']} {self.yaml_info_list['node'][1]['ip']} --node-type Combined",self.obj_satellite01)
        print(f"创建Combined节点{self.nodename_list[2]}")
        utils.exec_cmd(f"linstor node create {self.yaml_info_list['node'][2]['name']} {self.yaml_info_list['node'][2]['ip']} --node-type Combined",self.obj_satellite02)

    def linstor_r_2diskful_1diskless(self):
        print(f"创建rd和vd;vd的容量为:{self.data_list[0][1]}")
        utils.exec_cmd(f"linstor rd c {self.resource_name}",self.obj_list[0])
        utils.exec_cmd(f"linstor vd c {self.resource_name} {self.data_list[0][1]}",self.obj_list[0])
        print(f"在节点{self.nodename_list[0]}创建diskful资源")
        utils.exec_cmd(f"linstor r c {self.nodename_list[0]} {self.resource_name} --storage-pool {self.thick_lvm_storagepool_name}",self.obj_controller)
        print(f"在节点{self.nodename_list[1]}创建diskful资源")
        utils.exec_cmd(f"linstor r c {self.nodename_list[1]} {self.resource_name} --storage-pool {self.thick_lvm_storagepool_name}",self.obj_satellite01)
        print(f"在节点{self.nodename_list[2]}创建diskless资源")
        utils.exec_cmd(f"linstor r c {self.nodename_list[-1]} {self.resource_name} --diskless",self.obj_satellite02)


def main():
    print("------------测试开始：基础测试测试------------")
    Test = MainOperation()
    linstor_node_create.main()
    linstor_node_delete.main()
    Test.linstor_node_create_s()
    linstor_sp_thinlvm_create.main()
    linstor_sp_thinlvm_increase.main()
    linstor_sp_thinlvm_delete.main()
    linstor_sp_lvm_create.main()
    linstor_sp_lvm_delete.main()
    linstor_sp_lvm_create.main()
    linstor_r_create.main()
    Test._init_datalist()
    Test.linstor_r_2diskful_1diskless()
    drbd_status.main()
    linstor_r_decrease.main()
    linstor_r_increase.main()
    linstor_r_promotion.main()
    linstor_report_error.main()
    linstor_report_sos.main()
    linstor_responsetime.main()
    linstor_r_eviction.main()
    trim.main()


    print("------------测试结束：基础测试测试------------")

if __name__ == "__main__":
    main()
