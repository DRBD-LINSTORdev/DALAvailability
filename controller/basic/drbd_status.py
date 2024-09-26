from .base import BaseClass
from utils import utils
import time

class MainOperation(BaseClass):
    def __init__(self):
        super().__init__()

    def s_exec_cmd(self, cmd, ssh):
        transport = ssh.get_transport()
        channel = transport.open_session()
        channel.get_pty()
        channel.invoke_shell()
        channel.send(cmd + '\n')
        time.sleep(5)  # 等待5秒
        channel.send('\x03')  # 发送ctrl+c信号
        time.sleep(1)  # 等待1秒以确保命令已经被中断
        output = channel.recv(9999).decode()  # 获取命令的输出
        channel.close()
        return output


    def check_status(self):
        print("drbdadm status")
        utils.exec_cmd("drbdadm status",self.obj_controller)
        print(f"drbdadm status {self.resource_name}")
        utils.exec_cmd(f"drbdadm status {self.resource_name}",self.obj_controller)
        print(f"drbdsetup show {self.resource_name}")
        utils.exec_cmd(f"drbdsetup show {self.resource_name}",self.obj_controller)
        print("drbdsetup status")
        utils.exec_cmd("drbdsetup status",self.obj_controller)
        print("drbdsetup status -vs")
        utils.exec_cmd("drbdsetup status -vs",self.obj_controller)
        print("drbdsetup events2")
        result01 = self.s_exec_cmd("drbdsetup events2",self.obj_controller.sshconnection)
        log_data01 = f'{self.obj_list[0]._host} - {f"drbdsetup events2"} - {result01}'
        utils.Log().logger.info(log_data01)

def main():
    print("------------测试开始：DRBD 资源检查测试-----------")
    Test = MainOperation()
    Test.check_status()
    print("------------测试结束：DRBD 资源检查测试------------")


if __name__ == "__main__":
    main()