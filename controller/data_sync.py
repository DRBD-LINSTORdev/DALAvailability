import time
from .. import resources_operator
from .. import utils
from .. import exec_command
from .. import network_operator
import re
from .. import config_file
import sys
from threading import Thread

class All_operator:
    def __init__(self,node_a_ip,node_b_ip,node_c_ip,node_a_objssh,node_b_objssh,node_c_objssh,drbd_device_name="/dev/drbd1000"):
        self.a_ip = node_a_ip
        self.b_ip = node_b_ip
        self.c_ip = node_c_ip
        self.a_objssh = node_a_objssh
        self.b_objssh = node_b_objssh
        self.c_objssh = node_c_objssh
        self.drbd_device_name = drbd_device_name

    def record_a_to_b(self,source_record_path="/root/record.txt",target_record_path="/root/"):
        cmd = f"scp -r {source_record_path} root@{self.b_ip}:{target_record_path}"
        utils.exec_cmd(cmd,self.a_objssh)

    def record_a_to_c(self,source_record_path="/root/record.txt",target_record_path="/root/"):
        cmd = f"scp -r {source_record_path} root@{self.c_ip}:{target_record_path}"
        utils.exec_cmd(cmd, self.a_objssh)

    def record_b_to_a(self,source_record_path="/root/record.txt",target_record_path="/root/"):
        cmd = f"scp -r {source_record_path} root@{self.a_ip}:{target_record_path}"
        utils.exec_cmd(cmd, self.b_objssh)

    def record_c_to_a(self,source_record_path="/root/record.txt",target_record_path="/root/"):
        cmd = f"scp -r {source_record_path} root@{self.a_ip}:{target_record_path}"
        utils.exec_cmd(cmd, self.c_objssh)

    def record_c_to_b(self,source_record_path="/root/record.txt",target_record_path="/root/"):
        cmd = f"scp -r {source_record_path} root@{self.b_ip}:{target_record_path}"
        utils.exec_cmd(cmd, self.c_objssh)

    def record_delete_a(self):
        cmd = f"rm /root/record.txt"
        utils.exec_cmd(cmd, self.a_objssh)

    def record_delete_b(self):
        cmd = f"rm /root/record.txt"
        utils.exec_cmd(cmd, self.b_objssh)

    def record_delete_c(self):
        cmd = f"rm /root/record.txt"
        utils.exec_cmd(cmd, self.c_objssh)

    def dd_clear_a(self):
        cmd = f"dd if=/dev/zero of={self.drbd_device_name} oflag=direct status=progress bs=2M"
        utils.exec_cmd(cmd, self.a_objssh)

    def dd_clear_b(self):
        cmd = f"dd if=/dev/zero of={self.drbd_device_name} oflag=direct status=progress bs=2M"
        utils.exec_cmd(cmd, self.b_objssh)

    def dd_clear_c(self):
        cmd = f"dd if=/dev/zero of={self.drbd_device_name} oflag=direct status=progress bs=2M"
        utils.exec_cmd(cmd, self.c_objssh)

    def go_meter_write_a(self):
        cmd = f"/go-meter/main write"
        utils.exec_cmd(cmd, self.a_objssh)

    def go_meter_write_b(self):
        cmd = f"/go-meter/main write"
        utils.exec_cmd(cmd, self.b_objssh)

    def go_meter_write_c(self):
        cmd = f"/go-meter/main write"
        utils.exec_cmd(cmd, self.c_objssh)

    def go_meter_compare_a(self):
        cmd = f"/go-meter/main compare"
        utils.exec_cmd(cmd, self.a_objssh)

    def go_meter_compare_b(self):
        cmd = f"/go-meter/main compare"
        utils.exec_cmd(cmd, self.b_objssh)

    def go_meter_compare_c(self):
        cmd = f"/go-meter/main compare"
        utils.exec_cmd(cmd, self.c_objssh)

class Start:
    def __init__(self):
        self.obj_yaml = config_file.ConfFile('../config.yaml')
        self.yaml_info_list = self.obj_yaml.read_yaml()
        self.obj_a = exec_command.SSHconn(host=self.yaml_info_list['node'][0]['ip']
                                          , username=self.yaml_info_list['node'][0]['username']
                                          , password=self.yaml_info_list['node'][0]['password'])
        self.obj_b = exec_command.SSHconn(host=self.yaml_info_list['node'][1]['ip']
                                          , username=self.yaml_info_list['node'][1]['username']
                                          , password=self.yaml_info_list['node'][1]['password'])
        self.obj_c = exec_command.SSHconn(host=self.yaml_info_list['node'][2]['ip']
                                          , username=self.yaml_info_list['node'][2]['username']
                                          , password=self.yaml_info_list['node'][2]['password'])
        self.obj_all_operator = All_operator(node_a_ip=self.yaml_info_list['node'][0]['ip']
                                             , node_b_ip=self.yaml_info_list['node'][1]['ip']
                                             , node_c_ip=self.yaml_info_list['node'][2]['ip']
                                             , node_a_objssh=self.obj_a
                                             , node_b_objssh=self.obj_b
                                             , node_c_objssh=self.obj_c)

    def main(self):
        print("多线程：在a节点使用go-meter写")
        state1 = Thread(target=self.obj_all_operator.go_meter_write_a)
        state1.setDaemon(True)
        state1.start()
        time.sleep(20)
        print("关闭b节点的drbd网络")
        utils.exec_cmd(f"nmcli device disconnect {self.yaml_info_list['node'][1]['nic']}", self.obj_b)
        time.sleep(20)
        print("开启b节点的drbd网络")
        utils.exec_cmd(f"nmcli device connect {self.yaml_info_list['node'][1]['nic']}", self.obj_b)
        a = False
        while a is False:
            time.sleep(20)
            info = utils.exec_cmd("linstor r l",self.obj_c)
            status_result1 = re.findall(r'SyncTarget', info)
            status_result2 = re.findall(r'UpToDate', info)
            status_result3 = re.findall(r'InUse', info)
            if len(status_result3) == 1:
                print("go-meter写入中")
            elif len(status_result1) == 1:
                print("资源正在同步中")
            elif len(status_result2) == 3:
                print("资源同步结束")
                break
        print("将record从a移动到b节点")
        self.obj_all_operator.record_a_to_b()
        print("在a节点使用dd清除")
        self.obj_all_operator.dd_clear_a()
        print("在a节点进行go-meter比较")
        self.obj_all_operator.go_meter_compare_a()
        print("在b节点使用dd清除")
        self.obj_all_operator.dd_clear_b()
        print("在b节点进行go-meter比较")
        self.obj_all_operator.go_meter_compare_b()

        print("删除a节点的record")
        self.obj_all_operator.dd_clear_a()
        print("删除b节点的record")
        self.obj_all_operator.dd_clear_b()
#////////////////////////////////////////////
        print("多线程：在a节点使用go-meter写")
        state2 = Thread(target=self.obj_all_operator.go_meter_write_a)
        state2.setDaemon(True)
        state2.start()
        time.sleep(20)
        print("关闭a节点的drbd网络")
        utils.exec_cmd(f"nmcli device disconnect {self.yaml_info_list['node'][0]['nic']}", self.obj_a)
        a = False
        while a is False:
            time.sleep(5)
            if state2.is_alive() is True:
                print("go-meter运行中")
            else:
                print("go-meter结束")
                break
        print("多线程：在b节点使用go-meter写")
        state3 = Thread(target=self.obj_all_operator.go_meter_write_b)
        state3.setDaemon(True)
        state3.start()
        time.sleep(20)
        print("开启a节点的drbd网络")
        utils.exec_cmd(f"nmcli device connect {self.yaml_info_list['node'][0]['nic']}", self.obj_a)
        a = False
        while a is False:
            time.sleep(5)
            if state3.is_alive() is True:
                print("go-meter运行中")
            else:
                print("go-meter结束")
                break
        print("将record从b移动到a节点")
        self.obj_all_operator.record_b_to_a()
        print("在a节点使用dd清除")
        self.obj_all_operator.dd_clear_a()
        print("在a节点进行go-meter比较")
        self.obj_all_operator.go_meter_compare_a()
        print("在b节点使用dd清除")
        self.obj_all_operator.dd_clear_b()
        print("在b节点进行go-meter比较")
        self.obj_all_operator.go_meter_compare_b()

        print("删除a节点的record")
        self.obj_all_operator.record_delete_a()
        print("删除b节点的record")
        self.obj_all_operator.record_delete_b()
# ////////////////////////////////////////////
        print("多线程：在c节点使用go-meter写")
        state4 = Thread(target=self.obj_all_operator.go_meter_write_c)
        state4.setDaemon(True)
        state4.start()
        time.sleep(20)
        print("关闭b节点的drbd网络")
        utils.exec_cmd(f"nmcli device disconnect {self.yaml_info_list['node'][1]['nic']}", self.obj_b)
        time.sleep(20)
        print("开启b节点的drbd网络")
        utils.exec_cmd(f"nmcli device connect {self.yaml_info_list['node'][1]['nic']}", self.obj_b)
        a = False
        while a is False:
            time.sleep(5)
            if state4.is_alive() is True:
                print("go-meter运行中")
            else:
                print("go-meter结束")
                break
        print("将record从c移动到a节点")
        self.obj_all_operator.record_c_to_a()
        print("将record从c移动到b节点")
        self.obj_all_operator.record_c_to_b()

        print("删除a节点的record")
        self.obj_all_operator.record_delete_a()
        print("删除b节点的record")
        self.obj_all_operator.record_delete_b()
        print("删除c节点的record")
        self.obj_all_operator.record_delete_c()

def main():
    test = Start()
    test.main()

if __name__ == "__main__":
    main()
