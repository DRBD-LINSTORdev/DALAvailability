import sys
import re
from .base import BaseClass
from utils import utils



class MainOperation(BaseClass):
    """
    前提条件：
    1.必须有2diskful+1diskless
    """
    def __init__(self):
        super().__init__()
        self.data_list = []
        self._init_datalist()
        self._init_checkout()

    def _init_datalist(self):
        pass

    def _init_checkout(self):
        try:
            result = utils.exec_cmd(f"linstor r l",self.obj_list[0])
            result01 = re.findall(r'(UpToDate)', result)
            result02 = re.findall(r'(Diskless)', result)
            if len(result01) == 2 and len(result02) == 1:
                print("Trim/discard support测试，环境检查：2diskful+1diskless")
            else:
                print("Trim/discard support，环境检查：失败")
                sys.exit()
        except:
            print("Trim/discard support，环境检查：失败")
            sys.exit()


    def trim_test(self):
        data_list = []
        for i in range(len(self.obj_list)):
            result = utils.exec_cmd(f"cat /sys/block/drbd1000/queue/discard_max_bytes",self.obj_list[i])
            data_list.append(result.replace('\n', ''))

        if all(item.strip() == '0' for item in data_list):
            print(f'physical discard operation 符合预期:节点:{self.nodename_list[0]}的diskful资源1：{data_list[0]},节点:{self.nodename_list[1]}的diskful资源2：{data_list[1]},节点:{self.nodename_list[-1]}的diskless资源：{data_list[-1]}')
        elif (data_list[0].strip() != '0' or data_list[1].strip() != '0') and data_list[2].strip() != '0':
            print(f'physical discard operation 符合预期:节点:{self.nodename_list[0]}的diskful资源1：{data_list[0]},节点:{self.nodename_list[1]}的diskful资源2：{data_list[1]},节点:{self.nodename_list[-1]}的diskless资源：{data_list[-1]}')
        else:
            print(f"physical discard operation 不符合预期:节点:{self.nodename_list[0]}的diskful资源1：{data_list[0]},节点:{self.nodename_list[1]}的diskful资源2：{data_list[1]},节点:{self.nodename_list[-1]}的diskless资源：{data_list[-1]}")


def main():
    print("------------测试开始：Trim/discard support------------")
    Test = MainOperation()
    Test.trim_test()
    print("------------测试结束：Trim/discard support------------")

if __name__ == "__main__":
    main()