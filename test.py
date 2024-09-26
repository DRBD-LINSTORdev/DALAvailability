import re
import time

data = """
test09 role:Primary
  disk:UpToDate
"""
nodename_list = ["node12204","node22204","node32204"]
data_list = []
resource_name = "test01"

re_resoult01 = re.findall(r'test09 role:Primary', data)
print(re_resoult01)
if re_resoult01[0] != 'test09 role:primary':
    print("drbd资源状态异常")


# def test(data, nodename):
#     result1 = re.findall(r'\|\s+(\S+)\s+\|\s+%s'%nodename, data)
#     result1_1 = re.findall(r'%s\s+\|\s+\S+\s+\|\s+\S+\s+\|\s+(\S+)\s+(\S+)\s+\|'%nodename, data)
#     sp_name = result1[-1]
#     r_size = int(float(result1_1[1][0])) // 2 + 1
#     r_size_1 = str(r_size) + result1_1[1][1]
#     return sp_name,r_size_1
#
#
# for i in range(len(nodename_list)):
#     data1 = test(data, nodename_list[i])
#     data_list.append(data1)

#         time_result = self.obj_list[0].exec_cmd(f'time linstor n l')
#         log_data01 = f'{self.obj_list[0]._host} - {f"time linstor n l"} - {time_result}'
#         utils.Log().logger.info(log_data01)