import sys
import re
import random
from .base import BaseClass
from utils import utils



class MainOperation(BaseClass):
    """
    前提条件：

    """
    def __init__(self):
        super().__init__()
        self.data_list = []
        self.error_ip_list = []
        self._init_datalist()
        self._init_error_ip_list()


    def _init_datalist(self):
        pass

    def _init_error_ip_list(self):
        ip_parts = self.controller_ip.split('.')
        ips = []
        last_parts = random.sample(range(10, 251), 4)
        for i in last_parts:
            new_ip_parts = ip_parts[:-1] + [str(i)]
            new_ip = '.'.join(new_ip_parts)
            if new_ip != self.controller_ip:
                ips.append(new_ip)
            if len(ips) == 4:  # 当生成了4个IP地址后停止
                break
        self.error_ip_list = ips
        return ips

    def _processing_response_time(self,info):
        result = re.findall(r'\nreal\t([\w.]+)\n', info)

        return result[0]


    def controller_test_01(self):
        content = f"[global]\ncontrollers={self.controller_ip}"
        utils.exec_cmd(f'echo "{content}" > /etc/linstor/linstor-client.conf',self.obj_list[0])
        time_result = self.obj_list[0].exec_cmd(f'time linstor n l')
        log_data01 = f'{self.obj_list[0]._host} - {f"time linstor n l"} - {time_result}'
        utils.Log().logger.info(log_data01)
        print(f'linstor-controller:linstor-client中配置的第一个节点为有效controller地址: {self._processing_response_time(time_result["rt"])}')

    def controller_test_02(self):
        content = f"[global]\ncontrollers={self.error_ip_list[0]},{self.controller_ip}"
        utils.exec_cmd(f'echo "{content}" > /etc/linstor/linstor-client.conf',self.obj_list[0])
        time_result = self.obj_list[0].exec_cmd(f'time linstor n l')
        log_data01 = f'{self.obj_list[0]._host} - {f"time linstor n l"} - {time_result}'
        utils.Log().logger.info(log_data01)
        print(f'linstor-controller:linstor-client中配置的第二个节点为有效controller地址: {self._processing_response_time(time_result["rt"])}')

    def controller_test_03(self):
        content = f"[global]\ncontrollers={self.error_ip_list[0]},{self.error_ip_list[1]},{self.controller_ip}"
        utils.exec_cmd(f'echo "{content}" > /etc/linstor/linstor-client.conf',self.obj_list[0])
        time_result = self.obj_list[0].exec_cmd(f'time linstor n l')
        log_data01 = f'{self.obj_list[0]._host} - {f"time linstor n l"} - {time_result}'
        utils.Log().logger.info(log_data01)
        print(f'linstor-controller:linstor-client中配置的第三个节点为有效controller地址: {self._processing_response_time(time_result["rt"])}')

    def controller_test_04(self):
        content = f"[global]\ncontrollers={self.error_ip_list[0]},{self.error_ip_list[1]},{self.error_ip_list[2]},{self.controller_ip}"
        utils.exec_cmd(f'echo "{content}" > /etc/linstor/linstor-client.conf',self.obj_list[0])
        time_result = self.obj_list[0].exec_cmd(f'time linstor n l')
        log_data01 = f'{self.obj_list[0]._host} - {f"time linstor n l"} - {time_result}'
        utils.Log().logger.info(log_data01)
        print(f'linstor-controller:linstor-client中配置的第四个节点为有效controller地址: {self._processing_response_time(time_result["rt"])}')

    def satellite_test_01(self):
        content = f"[global]\ncontrollers={self.controller_ip}"
        utils.exec_cmd(f'echo "{content}" > /etc/linstor/linstor-client.conf',self.obj_list[1])
        time_result = self.obj_list[1].exec_cmd(f'time linstor n l')
        log_data01 = f'{self.obj_list[1]._host} - {f"time linstor n l"} - {time_result}'
        utils.Log().logger.info(log_data01)
        print(f'linstor-satellite:linstor-client中配置的第一个节点为有效controller地址: {self._processing_response_time(time_result["rt"])}')

    def satellite_test_02(self):
        content = f"[global]\ncontrollers={self.error_ip_list[0]},{self.controller_ip}"
        utils.exec_cmd(f'echo "{content}" > /etc/linstor/linstor-client.conf',self.obj_list[1])
        time_result = self.obj_list[1].exec_cmd(f'time linstor n l')
        log_data01 = f'{self.obj_list[1]._host} - {f"time linstor n l"} - {time_result}'
        utils.Log().logger.info(log_data01)
        print(f'linstor-satellite:linstor-client中配置的第二个节点为有效controller地址: {self._processing_response_time(time_result["rt"])}')

    def satellite_test_03(self):
        content = f"[global]\ncontrollers={self.error_ip_list[0]},{self.error_ip_list[1]},{self.controller_ip}"
        utils.exec_cmd(f'echo "{content}" > /etc/linstor/linstor-client.conf',self.obj_list[1])
        time_result = self.obj_list[1].exec_cmd(f'time linstor n l')
        log_data01 = f'{self.obj_list[1]._host} - {f"time linstor n l"} - {time_result}'
        utils.Log().logger.info(log_data01)
        print(f'linstor-satellite:linstor-client中配置的第三个节点为有效controller地址: {self._processing_response_time(time_result["rt"])}')

    def satellite_test_04(self):
        content = f"[global]\ncontrollers={self.error_ip_list[0]},{self.error_ip_list[1]},{self.error_ip_list[2]},{self.controller_ip}"
        utils.exec_cmd(f'echo "{content}" > /etc/linstor/linstor-client.conf',self.obj_list[1])
        time_result = self.obj_list[1].exec_cmd(f'time linstor n l')
        log_data01 = f'{self.obj_list[1]._host} - {f"time linstor n l"} - {time_result}'
        utils.Log().logger.info(log_data01)
        print(f'linstor-satellite:linstor-client中配置的第四个节点为有效controller地址: {self._processing_response_time(time_result["rt"])}')

    def restore_client(self):
        print(f"恢复/etc/linstor-client.conf")
        content = f"[global]\ncontrollers={self.controller_ip}"
        for i in range(len(self.obj_list)):
            utils.exec_cmd(f'echo "{content}" > /etc/linstor/linstor-client.conf', self.obj_list[i])





def main():
    print("------------测试开始：LINSTOR 响应时间测试------------")
    Test = MainOperation()
    Test.controller_test_01()
    Test.controller_test_02()
    Test.controller_test_03()
    Test.controller_test_04()
    Test.satellite_test_01()
    Test.satellite_test_02()
    Test.satellite_test_03()
    Test.satellite_test_04()
    Test.restore_client()
    print("------------测试结束：LINSTOR 响应时间测试------------")

if __name__ == "__main__":
    main()