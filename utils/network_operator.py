import utils
class DeviceService(object):

    def down_device(self,dev, ssh_conn=None):
        cmd = f'ifconfig {dev} down'
        if ssh_conn != None:
            result = utils.exec_cmd(cmd, ssh_conn)
            return result
        else:
            return cmd


    def up_device(self,dev, ssh_conn=None):
        cmd = f'nmcli device connect {dev}'
        if ssh_conn != None:
            result = utils.exec_cmd(cmd, ssh_conn)
            return result
        else:
            return cmd

    def disconn_device(self,dev, ssh_conn=None):
        cmd = f'nmcli dev disconnect {dev}'
        if ssh_conn != None:
            result = utils.exec_cmd(cmd, ssh_conn)
            return result
        else:
            return cmd