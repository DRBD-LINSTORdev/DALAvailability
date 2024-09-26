from .base import BaseClass
from utils import utils


class MainOperation(BaseClass):
    def __init__(self):
        super().__init__()


    def delete_sp(self):
        for i in range(len(self.obj_list)):
            print(f"删除节点:{self.obj_list[i]._name} 的thin lvm存储池:{self.thin_lvm_storagepool_name}")
            utils.exec_cmd(f"linstor sp d {self.nodename_list[i]} {self.thin_lvm_storagepool_name}",self.obj_list[i])


def main():
    Test = MainOperation()
    print("------------测试开始：thin lvm 存储池删除------------")
    Test.delete_sp()
    print("------------测试结束：thin lvm 存储池删除------------")


if __name__ == "__main__":
    main()