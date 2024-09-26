import sys
import re
from .base import BaseClass
from utils import utils


class MainOperation(BaseClass):
    """
    前提条件：

    """
    def __init__(self):
        super().__init__()
        self.data_list = []
        self._init_datalist()


    def _init_datalist(self):
        pass


    def error_list(self):
        print("linstor error-reports list执行")
        result = utils.exec_cmd(f"linstor error-reports list",self.obj_list[0])
        result01 = re.findall(r'(\w{8}-\w{5}-\w{6})', result)
        print("linstor error-reports show执行")
        utils.exec_cmd(f"linstor error-reports show {result01[0]}",self.obj_list[0])


def main():
    print("------------测试开始：LINSTOR error-reports测试------------")
    Test = MainOperation()
    Test.error_list()
    print("------------测试结束：LINSTOR error-reports测试------------")


if __name__ == "__main__":
    main()