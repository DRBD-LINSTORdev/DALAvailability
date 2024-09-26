import time
from .. import utils
from utils import exec_command, config_file, resources_operator
import re
import sys
from threading import Thread

class DdOperation:
    def __init__(self,obj_ssh,resource_name):
        self.linstor_cmds = resources_operator.Linstor()
        self.drbd_cmds = resources_operator.DRBD()
        self.lvm_cmds = resources_operator.LVM()
        self.obj_ssh = obj_ssh
        self.r_name = resource_name
        self.obj_dd = utils.RWData()

    def get_devicename(self):
        check_cmd = self.linstor_cmds.check_resource_detailed(self.r_name,self.obj_ssh)
        data = re.findall(f'\|([\w\s\/]+)', check_cmd)
        data1 = data[5]
        data2 = data1.strip()
        print(f"devicename已获取，为{data2}")
        return data2

    def use_dd_to_write_data(self): #用多线程重写
        try:
            devicename = self.get_devicename()
            print("................................线程1:开始执行dd写数据操作")
            dd_write = self.obj_dd.dd_write(devicename,self.obj_ssh)
            time.sleep(5)
            print(".....................dd写数据操作执行完毕,dd进程已被关闭")
        except:
            print("dd写数据操作执行失败")
            sys.exit()

    def user_dd_to_read_data(self):
        try:
            devicename = self.get_devicename()
            print("................................线程1:开始执行dd读数据操作")
            dd_read = self.obj_dd.dd_read(devicename,'./test.txt',self.obj_ssh)
            time.sleep(5)
            print(".....................dd读数据操作执行完毕,dd进程已被关闭")
        except:
            print("dd读数据操作执行失败")
            sys.exit()

    def start_dd_write(self):
        state1 = Thread(target=self.use_dd_to_write_data)
        state1.setDaemon(True)
        state1.start()

    def start_dd_read(self):
        state1 = Thread(target=self.user_dd_to_read_data())
        state1.setDaemon(True)
        state1.start()

    def stop_dd(self):
        try:
            process_info = utils.exec_cmd('ps -A | grep dd',self.obj_ssh)
            result01 = re.findall(f'([\d]+) \?        \w\w:\w\w:\w\w dd',process_info)
            r_dd_pid = result01[0]
            kill_dd_cmd = self.obj_dd.kill_dd(r_dd_pid)
            utils.exec_cmd(kill_dd_cmd,self.obj_ssh)
            print("dd进程已终止")
        except:
            print("停止dd写数据出现错误")
            sys.exit()




class MainOperation:
    """
    默认环境已经配置完成
    有node
    """
    def __init__(self):
        self.obj_yaml = config_file.ConfFile('../config.yaml')
        self.yaml_info_list = self.obj_yaml.read_yaml()
        self.linstor_cmds = resources_operator.Linstor()
        self.drbd_cmds = resources_operator.DRBD()
        self.lvm_cmds = resources_operator.LVM()
        self.obj_controller = exec_command.SSHconn(host=self.yaml_info_list['node'][0]['ip']
                                                   , username=self.yaml_info_list['node'][0]['username']
                                                   , password=self.yaml_info_list['node'][0]['password'])
        self.obj_satellite01 = exec_command.SSHconn(host=self.yaml_info_list['node'][1]['ip']
                                                    , username=self.yaml_info_list['node'][1]['username']
                                                    , password=self.yaml_info_list['node'][1]['password'])
        self.obj_satellite02 = exec_command.SSHconn(host=self.yaml_info_list['node'][2]['ip']
                                                    , username=self.yaml_info_list['node'][2]['username']
                                                    , password=self.yaml_info_list['node'][2]['password'])

    def create_50G_r(self):
        """
        创建资源:创建sp,rd,vd,r
        """
        create_sp1 = self.linstor_cmds.create_sp(self.yaml_info_list['node'][0]['name'],'lvm','sptest','vgtest',self.obj_controller)
        create_sp2 = self.linstor_cmds.create_sp(self.yaml_info_list['node'][1]['name'],'lvm','sptest','vgtest',self.obj_satellite01)
        create_sp3 = self.linstor_cmds.create_sp(self.yaml_info_list['node'][2]['name'],'lvm','sptest','vgtest',self.obj_satellite02)

        create_rd = self.linstor_cmds.create_rd('resourcetest01',self.obj_controller)
        create_vd = self.linstor_cmds.create_vd('resourcetest01','50G',self.obj_controller)

        create_diskful1 = self.linstor_cmds.create_diskful_resource(self.yaml_info_list['node'][1]['name'],'resourcetest01','sptest',self.obj_satellite01)
        create_diskful2 = self.linstor_cmds.create_diskful_resource(self.yaml_info_list['node'][2]['name'],'resourcetest01','sptest',self.obj_satellite02)
        create_diskless = self.linstor_cmds.create_diskless_resource(self.yaml_info_list['node'][0]['name'],'resourcetest01',self.obj_controller)


    def test(self):

        check_result01 = self.linstor_cmds.check_resource(self.obj_controller)
        in_node_name_f = re.findall(r'resourcetest01[\s]*┊[\s]*([\w]*)[\s]*┊[\s]*[\w]*[\s]*┊[\s]*[\w]*[\s]*┊[\s]*[\w]*[\s]*┊[\s]*SyncTarget', str(check_result01))
        in_node_name = in_node_name_f[0]

        for i in self.yaml_info_list['node']:
            if i['name'] == in_node_name:
                node_info = i
                break

        in_ssh_obj = exec_command.SSHconn(host=node_info['ip']
                                          , username=node_info['username']
                                          , password=node_info['password'])

        self.drbd_cmds.stop_sync("resourcetest01",in_ssh_obj)
        time.sleep(5)
        self.drbd_cmds.set_primary("resourcetest01",in_ssh_obj)
        time.sleep(5)
        check_result02 = self.drbd_cmds.drbdadm_status(in_ssh_obj)
        role_status_f = re.findall(r'fres role:([\w]*)\n', str(check_result02))
        if role_status_f[0] == 'Primary':
            print("resource status : Primary")
            time.sleep(5)
            self.drbd_cmds.set_secondary("resourcetest01",in_ssh_obj)
            print("resource status is set to : Secondary")
        elif role_status_f == 'Secondary':
            print("resource status : Secondary")
        else:
            print(f"resource status error : {role_status_f[0]}")

        obj_dd = DdOperation(in_ssh_obj,"resourcetest01")
        obj_dd.use_dd_to_write_data()
        time.sleep(5)
        obj_dd.stop_dd()
        time.sleep(2)
        obj_dd.user_dd_to_read_data()
        time.sleep(5)
        obj_dd.stop_dd()

        self.drbd_cmds.start_sync("resourcetest01",in_ssh_obj)

        node1_name = self.yaml_info_list['node'][1]['name']
        node2_name = self.yaml_info_list['node'][2]['name']
        info = self.linstor_cmds.check_resource(in_ssh_obj)
        result1 = re.findall(r'(%s)[\w\W]*(UpToDate)' % node1_name, info)
        result2 = re.findall(r'(%s)[\w\W]*(UpToDate)' % node2_name, info)
        a = False
        while a is False:
            try:
                if result1[0][0] == node1_name and result2[0][0] == node2_name:
                    print("sync complete")
                    break
                else:
                    print("sync failed")
                    break

            except:
                time.sleep(30)
                info = self.linstor_cmds.check_resource(in_ssh_obj)
                result1 = re.findall(r'(%s)[\w\W]*(UpToDate)' % node1_name, info)
                result2 = re.findall(r'(%s)[\w\W]*(UpToDate)' % node2_name, info)
                print('Synchronizing........')
                continue
def main():
    obj_main_operation = MainOperation()
    obj_main_operation.create_50G_r()
    obj_main_operation.test()

if __name__ == "__main__":
    main()


        # re.findall(r'(%s)[\s]*\|[\w\s]*\|[\w\s]*\|[\w\s(),]*\|([\w\s().%%]*)\|' % node1_name, str(info))