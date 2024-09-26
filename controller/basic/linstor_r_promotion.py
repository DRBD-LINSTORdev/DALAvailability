from .base import BaseClass
from utils import utils
import re
import sys
import time
from threading import Thread


class MainOperation(BaseClass):
    """
    前提条件：
        确保2diskful+1diskless
    """
    def __init__(self):
        super().__init__()
        self.drbd_device = self._get_drbddevice()

    def _get_drbddevice(self):
        r_info = utils.exec_cmd(f"linstor r lv",self.obj_list[0])
        drbddevice = re.findall(r'%s\s+\|\s+\S+\s+\|\s+\S+\s+\|\s+\S+\s+\|\s+\S+\s+\|\s+(\S+)' % self.nodename_list[0], r_info)

        return drbddevice[0]

    def _check_1primary_2secondary(self,obj_ssh):
        drbdadm_status = utils.exec_cmd(f"drbdadm status",obj_ssh)

        result1 = re.findall(self.resource_name + r'\s+(role:Primary)', drbdadm_status)
        result2 = re.findall(r'(role:Secondary)', drbdadm_status)

        if len(result1) == 1 and len(result2) == 2:
            print(f"drbdadm status 资源检查正常:{obj_ssh._name}检查的资源状态:Primary，其他两个节点:Secondary")
        else:
            print("drbdadm status 资源检查异常")
            sys.exit()

    def _check_3secondary(self,obj_ssh):
        drbdadm_status = utils.exec_cmd(f"drbdadm status",obj_ssh)

        result2 = re.findall(r'(role:Secondary)', drbdadm_status)

        if len(result2) == 3:
            print(f"drbdadm status 资源检查正常,{obj_ssh._name}检查的资源状态:3个节点都为Secondary")
        else:
            print("drbdadm status 资源检查异常")
            sys.exit()


    def _dd_write(self, obj_ssh):
        print(f"......线程1:节点{obj_ssh._name}开始执行dd写数据操作")
        log_data01 = f'{obj_ssh._host} - {f"dd if=/dev/urandom of={self.drbd_device} oflag=direct status=progress"} - {""}'
        utils.Log().logger.info(log_data01)
        dd_write = utils.exec_cmd(f"dd if=/dev/urandom of={self.drbd_device} oflag=direct status=progress",obj_ssh)
        print("......dd写数据操作执行完毕,dd进程已被关闭")


    def _dd_stop(self, obj_ssh):
        try:
            process_info = utils.exec_cmd('ps -A | grep dd', obj_ssh)
            result01 = re.findall(f'(\S+)\s+\S+\s+\S+\s+dd', process_info)
            r_dd_pid = result01[0]
            utils.exec_cmd(f"kill -9 {r_dd_pid}", obj_ssh)
            print("dd进程已终止")
        except:
            print("停止dd写数据出现错误")
            sys.exit()

    def _thread_dd_write(self, obj_ssh):
        state = Thread(target=self._dd_write, args=(obj_ssh,))
        state.setDaemon(True)
        state.start()


    def create_2diskful1diskless(self):
        pass


    def up_promotion(self):
        print("开启auto-promote")
        utils.exec_cmd(f"linstor rd drbd-options --auto-promote yes {self.resource_name}",self.obj_list[0])

    def down_promotion(self):
        print("关闭auto-promote")
        utils.exec_cmd(f"linstor rd drbd-options --auto-promote no {self.resource_name}",self.obj_list[0])

    def up_diskful_automatic_promotion_mount(self):
        print("------diskful_automatic_promotion_mount测试开始------")
        print(f"开始对资源{self.resource_name}:{self.drbd_device}进行格式化")
        result01 = self.obj_list[0].exec_cmd(f"mkfs.ext4 -F {self.drbd_device}")
        log_data01 = f'{self.obj_list[0]._host} - {f"mkfs.ext4 -F {self.drbd_device}"} - {result01}'
        utils.Log().logger.info(log_data01)
        if result01["st"] == False:
            print("mkfs.ext4状态正常")
        else:
            print(f"mkfs.ext4状态错误:{result01}")
            sys.exit()
        print(f"在节点{self.obj_list[0]._name}上进行mount {self.drbd_device} /mnt")
        utils.exec_cmd(f"mount {self.drbd_device} /mnt",self.obj_list[0])
        self._check_1primary_2secondary(self.obj_list[0])
        print(f"在节点{self.obj_list[0]._name}上进行umount {self.drbd_device}")
        utils.exec_cmd(f"umount {self.drbd_device}",self.obj_list[0])
        self._check_3secondary(self.obj_list[0])
        print("------diskful_automatic_promotion_mount测试结束------")

    def up_diskless_automatic_promotion_mount(self):
        print("------diskless_automatic_promotion_mount测试开始------")
        print(f"开始对资源{self.resource_name}:{self.drbd_device}进行格式化")
        result01 = self.obj_list[-1].exec_cmd(f"mkfs.ext4 -F {self.drbd_device}")
        log_data01 = f'{self.obj_list[-1]._host} - {f"mkfs.ext4 -F {self.drbd_device}"} - {result01}'
        utils.Log().logger.info(log_data01)
        if result01["st"] == False:
            print("mkfs.ext4状态正常")
        else:
            print(f"mkfs.ext4状态错误:{result01}")
            sys.exit()
        print(f"在节点{self.obj_list[-1]._name}上进行mount {self.drbd_device} /mnt")
        utils.exec_cmd(f"mount {self.drbd_device} /mnt",self.obj_list[-1])
        self._check_1primary_2secondary(self.obj_list[-1])
        print(f"在节点{self.obj_list[-1]._name}上进行umount {self.drbd_device}")
        utils.exec_cmd(f"umount {self.drbd_device}",self.obj_list[-1])
        self._check_3secondary(self.obj_list[-1])
        print("------diskless_automatic_promotion_mount测试结束------")

    def up_diskful_automatic_promotion_dd(self):
        print("------diskful_automatic_promotion_dd测试开始------")
        self._thread_dd_write(self.obj_list[0])
        time.sleep(5)
        self._check_1primary_2secondary(self.obj_list[0])
        self._dd_stop(self.obj_list[0])
        utils.exec_cmd(f"drbdadm secondary {self.resource_name}",self.obj_list[0])
        self._check_3secondary(self.obj_list[0])
        print("------diskful_automatic_promotion_dd测试结束------")

    def up_diskless_automatic_promotion_dd(self):
        print("------diskless_automatic_promotion_dd测试开始------")
        self._thread_dd_write(self.obj_list[-1])
        time.sleep(5)
        self._check_1primary_2secondary(self.obj_list[-1])
        self._dd_stop(self.obj_list[-1])
        utils.exec_cmd(f"drbdadm secondary {self.resource_name}",self.obj_list[-1])
        self._check_3secondary(self.obj_list[-1])
        print("------diskless_automatic_promotion_dd测试结束------")

    def down_diskful_automatic_promotion_mount(self):
        print("------diskful_automatic_promotion_mount测试开始------")
        print(f"开始对资源{self.resource_name}:{self.drbd_device}进行格式化")
        result01 = self.obj_list[0].exec_cmd(f"mkfs.ext4 -F {self.drbd_device}")
        log_data01 = f'{self.obj_list[0]._host} - {f"mkfs.ext4 -F {self.drbd_device}"} - {result01}'
        utils.Log().logger.info(log_data01)
        if result01["st"] == False:
            print("mkfs.ext4状态正常")
        else:
            print(f"mkfs.ext4状态错误:{result01}")
            sys.exit()
        utils.exec_cmd(f"drbdadm primary {self.resource_name}",self.obj_list[0])
        result02 = self.obj_list[0].exec_cmd(f"mkfs.ext4 -F {self.drbd_device}")
        log_data02 = f'{self.obj_list[0]._host} - {f"mkfs.ext4 -F {self.drbd_device}"} - {result02}'
        utils.Log().logger.info(log_data02)
        print(f"在节点{self.obj_list[0]._name}上进行mount {self.drbd_device} /mnt")
        utils.exec_cmd(f"mount {self.drbd_device} /mnt",self.obj_list[0])
        self._check_1primary_2secondary(self.obj_list[0])
        print(f"在节点{self.obj_list[0]._name}上进行umount {self.drbd_device}")
        utils.exec_cmd(f"umount {self.drbd_device}",self.obj_list[0])
        self._check_1primary_2secondary(self.obj_list[0])
        utils.exec_cmd(f"drbdadm secondary {self.resource_name}",self.obj_list[0])
        self._check_3secondary(self.obj_list[0])
        print("------diskful_automatic_promotion_mount测试结束------")

    def down_diskless_automatic_promotion_mount(self):
        print("------diskless_automatic_promotion_mount测试开始------")
        print(f"开始对资源{self.resource_name}:{self.drbd_device}进行格式化")
        result01 = self.obj_list[-1].exec_cmd(f"mkfs.ext4 -F {self.drbd_device}")
        log_data01 = f'{self.obj_list[-1]._host} - {f"mkfs.ext4 -F {self.drbd_device}"} - {result01}'
        utils.Log().logger.info(log_data01)
        if result01["st"] == False:
            print("mkfs.ext4状态正常")
        else:
            print(f"mkfs.ext4状态错误:{result01}")
            sys.exit()
        utils.exec_cmd(f"drbdadm primary {self.resource_name}",self.obj_list[-1])
        result02 = self.obj_list[-1].exec_cmd(f"mkfs.ext4 -F {self.drbd_device}")
        log_data02 = f'{self.obj_list[-1]._host} - {f"mkfs.ext4 -F {self.drbd_device}"} - {result02}'
        utils.Log().logger.info(log_data02)
        print(f"在节点{self.obj_list[-1]._name}上进行mount {self.drbd_device} /mnt")
        utils.exec_cmd(f"mount {self.drbd_device} /mnt",self.obj_list[-1])
        self._check_1primary_2secondary(self.obj_list[-1])
        print(f"在节点{self.obj_list[-1]._name}上进行umount {self.drbd_device}")
        utils.exec_cmd(f"umount {self.drbd_device}",self.obj_list[-1])
        self._check_1primary_2secondary(self.obj_list[-1])
        utils.exec_cmd(f"drbdadm secondary {self.resource_name}",self.obj_list[-1])
        self._check_3secondary(self.obj_list[-1])
        print("------diskless_automatic_promotion_mount测试结束------")

    def down_diskful_automatic_promotion_dd(self):
        print("------diskful_automatic_promotion_dd测试开始------")
        result = self.obj_list[0].exec_cmd(f"dd if=/dev/urandom of={self.drbd_device} oflag=direct status=progress")
        if result["st"] == False:
            pass
        else:
            print("dd状态错误")
            sys.exit()
        utils.exec_cmd(f"drbdadm primary {self.resource_name}",self.obj_list[0])
        self._thread_dd_write(self.obj_list[0])
        time.sleep(5)
        self._check_1primary_2secondary(self.obj_list[0])
        self._dd_stop(self.obj_list[0])
        utils.exec_cmd(f"drbdadm secondary {self.resource_name}", self.obj_list[0])
        self._check_3secondary(self.obj_list[0])
        print("------diskful_automatic_promotion_dd测试结束------")

    def down_diskless_automatic_promotion_dd(self):
        print("------diskless_automatic_promotion_dd测试开始------")
        result = self.obj_list[-1].exec_cmd(f"dd if=/dev/urandom of={self.drbd_device} oflag=direct status=progress")
        if result["st"] == False:
            pass
        else:
            print("dd状态错误")
            sys.exit()
        utils.exec_cmd(f"drbdadm primary {self.resource_name}",self.obj_list[-1])
        self._thread_dd_write(self.obj_list[-1])
        time.sleep(5)
        self._check_1primary_2secondary(self.obj_list[-1])
        self._dd_stop(self.obj_list[-1])
        utils.exec_cmd(f"drbdadm secondary {self.resource_name}", self.obj_list[-1])
        self._check_3secondary(self.obj_list[-1])
        print("------diskless_automatic_promotion_dd测试结束------")

def main():
    print("------------测试开始：auto-promotion------------")
    Test = MainOperation()
    Test.up_promotion()
    Test.up_diskful_automatic_promotion_mount()
    Test.up_diskless_automatic_promotion_mount()
    Test.up_diskful_automatic_promotion_dd()
    Test.up_diskless_automatic_promotion_dd()
    Test.down_promotion()
    Test.down_diskful_automatic_promotion_mount()
    Test.down_diskless_automatic_promotion_mount()
    Test.down_diskful_automatic_promotion_dd()
    Test.down_diskless_automatic_promotion_dd()
    print("------------测试结束：auto-promotion------------")

if __name__ == "__main__":
    main()