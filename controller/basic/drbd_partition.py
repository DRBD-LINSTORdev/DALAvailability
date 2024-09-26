import re
import sys
from .base import BaseClass
from utils import utils


class MainOperation(BaseClass):
    def __init__(self):
        super().__init__()

    def drbd_disk_create_res(self):
        print("开始创建res文件:/etc/drbd.d/test09.res;和expect脚本:/root/create_md.exp")
        file_path = "/etc/drbd.d/test09.res"

        file_content = f"""resource test09 {{
    on {self.nodename_list[0]} {{
        device /dev/drbd9;
        disk {self.yaml_info_list['node'][0]['partition_path']};
        address {self.obj_list[0]._host}:7789;
        node-id 0;
        meta-disk internal;
    }}
}}"""

        script_cmd = f"""cat > /root/create_md.exp << EOF
#!/usr/bin/expect -f

spawn drbdadm create-md test09
expect "Do you want to proceed?\r\n\[need to type 'yes' to confirm\]"
send "yes\r"
expect eof
EOF"""

        # 使用echo命令将文件内容写入文件
        utils.exec_cmd(f'echo "{file_content}" > {file_path}',self.obj_list[0])
        utils.exec_cmd(f'{script_cmd}',self.obj_list[0])
        utils.exec_cmd(f"chmod +x /root/create_md.exp",self.obj_list[0])
        print("res文件和expect脚本创建完成")
        print("执行drbdadm create-md test09")
        utils.exec_cmd(f"/root/create_md.exp",self.obj_list[0])




    def drbd_disk_create_resource(self):
        print("执行drbdadm up test09")
        utils.exec_cmd("drbdadm up test09",self.obj_controller)
        print("执行drbdadm primary --force test09")
        utils.exec_cmd("drbdadm primary --force test09",self.obj_controller)
        print("检查drbd资源状态")
        resoult01 = utils.exec_cmd("drbdadm status test09",self.obj_controller)
        re_resoult01 = re.findall(r'test09 role:Primary',resoult01)
        if re_resoult01[0] != 'test09 role:Primary':
            print("drbd资源状态异常")
            sys.exit()
        else:
            print("drbd资源状态正常为:Primary")
        print("清理环境:drbdadm down test09")
        utils.exec_cmd("drbdadm down test09",self.obj_controller)
        print("清理环境:rm /etc/drbd.d/test09.res")
        utils.exec_cmd("rm /etc/drbd.d/test09.res",self.obj_controller)
        print("清理环境:rm /root/create_md.exp")
        utils.exec_cmd("rm /root/create_md.exp",self.obj_controller)

def main():
    Test = MainOperation()
    print(f"------------开始测试：drbd资源创建（分区）,测试节点:{Test.obj_list[0]._name},使用盘:{Test.yaml_info_list['node'][0]['partition_path']}------------")
    Test.drbd_disk_create_res()
    Test.drbd_disk_create_resource()
    print("------------测试结束：drbd资源创建（分区）------------")

if __name__ == "__main__":
    main()