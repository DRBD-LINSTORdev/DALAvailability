import sys
import re
from .base import BaseClass
from utils import utils


class MainOperation(BaseClass):
    """
    """
    def __init__(self):
        super().__init__()
        self.data_list = []
        self._init_datalist()


    def _init_datalist(self):
        sp_info = utils.exec_cmd(f"linstor sp l",self.obj_list[0])
        vd_info = utils.exec_cmd(f"linstor vd l",self.obj_list[0])

        vd_result01 = re.findall(f'%s\s+\|\s+(\S+)\s+\|\s+\S+\s+\|\s+(\S+)'%self.resource_name, vd_info)

        for i in range(len(self.obj_list)):
            total_data = list(self._calculate_r_size(sp_info, self.nodename_list[i]))
            total_data.append(vd_result01[0][1])
            sum_value = int(total_data[1]) + int(total_data[-1])
            new_value = str(sum_value) + total_data[2]
            new_arr = [total_data[0], new_value]
            new_arr.append(vd_result01[0][0])

            self.data_list.append(new_arr)

    def _calculate_r_size(self, info, nodename):
        try:
            result1 = re.findall(r'\|\s+(\S+)\s+\|\s+%s' % nodename, info)
            result1_1 = re.findall(r'%s\s+\|\s+\S+\s+\|\s+\S+\s+\|\s+(\S+)\s+(\S+)\s+\|' % nodename, info)
            sp_name = result1[-1]
            r_size = int(float(result1_1[1][0])) // 2 + 1
            r_size_1 = str(r_size // 2 + 1)
            r_unit = result1_1[1][1]
            return sp_name, r_size_1, r_unit
        except Exception as r :
            print(f"计算创建resource容量错误\n{r}")
            sys.exit()

    def increase_resource(self):
        print(f"对资源:{self.resource_name}进行扩容，扩容后的大小为:{self.data_list[0][1]}")
        utils.exec_cmd(f"linstor vd set-size {self.resource_name} {self.data_list[0][-1]} {self.data_list[0][1]}",self.obj_list[0])


def main():
    print("------------测试开始：资源扩容------------")
    print("计算资源需要扩容的大小")
    Test = MainOperation()
    Test.increase_resource()
    print("------------测试结束：资源扩容------------")


if __name__ == "__main__":
    main()