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


    def sos_(self):
        utils.exec_cmd(f"linstor sos-report download --since 3days",self.obj_list[0])



def main():
    print("------------测试开始：LINSTOR sos-reports测试------------")
    Test = MainOperation()
    Test.sos_()
    print("------------测试结束：LINSTOR sos-reports测试------------")


if __name__ == "__main__":
    main()