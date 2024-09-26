from .base import BaseClass
from utils import utils

class MainOperation(BaseClass):
    """
    前提条件：
        有资源存在
    """
    def __init__(self):
        super().__init__()


    def delete_r(self):
        print(f"删除资源:{self.resource_name}")
        utils.exec_cmd(f"linstor rd d {self.resource_name}",self.obj_list[0])

def main():
    print("------------测试开始：资源删除------------")
    Test = MainOperation()
    Test.delete_r()
    print("------------测试结束：资源删除------------")

if __name__ == "__main__":
    main()