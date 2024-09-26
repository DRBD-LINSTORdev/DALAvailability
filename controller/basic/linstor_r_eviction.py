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
        self.new_obj = None

    def _create_new_sshobj(self):
        self.new_obj = utils.exec_command.SSHconn(host=self.yaml_info_list['node'][1]['ip']
                                                    , name=self.yaml_info_list['node'][1]['name']
                                                    , username=self.yaml_info_list['node'][1]['username']
                                                    , password=self.yaml_info_list['node'][1]['password'])

    def _get_drbddevice(self):
        r_info = utils.exec_cmd(f"linstor r lv",self.obj_list[0])
        drbddevice = re.findall(r'%s\s+\|\s+\S+\s+\|\s+\S+\s+\|\s+\S+\s+\|\s+\S+\s+\|\s+(\S+)' % self.nodename_list[0], r_info)

        return drbddevice[0]

    def _up_eviction(self):
        utils.exec_cmd(f"linstor controller sp DrbdOptions/AutoEvictAllowEviction true",self.obj_list[0])

    def _down_bmc(self, ip, username, password):
        print(f"开始关闭节点：{self.obj_list[1]._name}的bmc")
        utils.exec_cmd(f"ipmitool -I lanplus -H {ip} -U {username} -P {password} power off",self.obj_list[-1])
        flag = False
        for i in range(18):
            time.sleep(10)
            bmc_status = utils.exec_cmd(f"ipmitool -I lanplus -H {ip} -U {username} -P {password} power status",self.obj_list[-1])
            if "Chassis Power is off" in bmc_status:
                flag = True
                break
        if flag:
            print("bmc被成功关闭")
        else:
            print("bmc关闭失败")

    def _up_bmc(self, ip, username, password):
        print(f"开始开启节点：{self.obj_list[1]._name}的bmc")
        utils.exec_cmd(f"ipmitool -I lanplus -H {ip} -U {username} -P {password} power on",self.obj_list[-1])
        flag = False
        for i in range(18):
            time.sleep(10)
            bmc_status = utils.exec_cmd(f"ipmitool -I lanplus -H {ip} -U {username} -P {password} power status",self.obj_list[-1])
            if "Chassis Power is on" in bmc_status:
                flag = True
                break
        if flag:
            print("bmc被成功开启")
        else:
            print("bmc开启失败")


    def check_evictied(self,obj_ssh, node_name):
        node_info = utils.exec_cmd(f"linstor n l",obj_ssh)
        result = re.findall(r'%s\s+\|\s+\S+\s+\|\s+\S+\s+\S+\s+\|\s+(\S+)'%node_name,node_info)
        if "EVICTED" in result[0]:
            print(f"关闭节点的状态为:EVICTED")
        else:
            print(f"关闭节点的状态为:{result[0]}，异常")
            sys.exit()

    def restore_evitcted(self,obj_ssh):
        print("恢复evicted状态")
        utils.exec_cmd(f"systemctl restart linstor-satellite", obj_ssh)
        result01 = obj_ssh.exec_cmd(f'linstor node restore {obj_ssh._name}')
        log_data01 = f'{self.obj_ssh._host} - {f"linstor node restore {obj_ssh._name}"} - {result01}'
        utils.Log().logger.info(log_data01)

        flag = False
        for i in range(18):
            time.sleep(10)
            node_status = utils.exec_cmd(f"linstor n l",obj_ssh)
            result02 = re.findall(r'(Online)',node_status)
            if len(result02) == 3:
                flag = True
                break
        if flag:
            print(f"节点{obj_ssh._name}的状态恢复为Online")
        else:
            print(f"节点{obj_ssh._name}的状态异常")
            sys.exit()




def main():
    print("------------测试开始：auto-eviction------------")
    Test = MainOperation()
    Test._up_eviction()
    Test._down_bmc(ip=Test.bmc_list[1]["ip"]
                   ,username=Test.bmc_list[1]["username"]
                   ,password=Test.bmc_list[1]["password"])
    print("开始等待90分钟")
    time.sleep(5400)
    Test.check_evictied(Test.obj_list[0],Test.nodename_list[1])
    Test._up_bmc(ip=Test.bmc_list[1]["ip"]
               ,username=Test.bmc_list[1]["username"]
               ,password=Test.bmc_list[1]["password"])
    time.sleep(300)
    Test._create_new_sshobj()
    Test.restore_evitcted(Test.new_obj)
    print("------------测试结束：auto-eviction------------")

if __name__ == "__main__":
    main()

