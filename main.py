import argparse
from controller import basic,core

# ----以下是一级和二级命令行----
# python3 main.py
# # 一键执行全部测试
# python3 main.py basic(b)
# # 一键执行基础测试
# python3 main.py core(c)
# # 一键执行核心功能测试
#
# ----以下是基础测试的所有级命令行----
# python3 main.py basic(b) drbd(d)
# # 一键执行全部drbd资源相关测试
# python3 main.py basic(b) drbd(d) disk(d)
# # 后端存储为整盘来创建DRBD资源
# python3 main.py basic(b) drbd(d) partition(p)
# # 后端存储为分区来创建DRBD资源
# python3 main.py basic(b) drbd(d) status(s)
# # 通过各种检查命令来查看DRBD资源状态和配置
#
# python3 main.py basic(b) linstor(l)
# # 一键执行全部linstor相关测试
# python3 main.py basic(b) linstor(l) node(n) create(c)
# # 创建linstor集群节点
# python3 main.py basic(b) linstor(l) node(n) delete(d)
# # 删除linstor集群节点
# python3 main.py basic(b) linstor(l) storagepool(sp) lvm(l) create(c)
# # 以thick lvm创建linstor存储池
# python3 main.py basic(b) linstor(l) storagepool(sp) lvm(l) delete(d)
# # 删除linstor thick lvm存储池
# python3 main.py basic(b) linstor(l) storagepool(sp) thinlvm(tl) create(c)
# # 以thin lvm创建linstor存储池
# python3 main.py basic(b) linstor(l) storagepool(sp) thinlvm(tl) delete(d)
# # 删除linstor thin lvm存储池
# python3 main.py basic(b) linstor(l) storagepool(sp) thinlvm(tl) increase(i)
# # linstor thin lvm存储池扩容
# python3 main.py basic(b) linstor(l) storagepool(sp) thinlvm(tl) decrease(d)
# # linstor thin lvm存储池缩减,没有此功能
# python3 main.py basic(b) linstor(l) resource(r) create(c)
# # linstor资源创建
# python3 main.py basic(b) linstor(l) resource(r) delete(d)
# # linstor资源删除
# python3 main.py basic(b) linstor(l) resource(r) promotion(p)
# # 禁用\开启 auto-promotion测试
# python3 main.py basic(b) linstor(l) resource(r) eviction(e)
# # 禁用 auto-eviction测试
# python3 main.py basic(b) linstor(l) resource(r) increase(i)
# # linstor资源扩容
# python3 main.py basic(b) linstor(l) resource(r) decrease(de)
# # linstor资源缩减
# python3 main.py basic(b) linstor(l) report(rp) error(e)
# # LINSTOR error-reports测试
# python3 main.py basic(b) linstor(l) report(rp) sos(s)
# # LINSTOR sos-report测试
# python3 main.py basic(b) linstor(l) responsetime(rs)
# # LINSTOR 响应时间测试
#
# python3 main.py basic(b) trim(t)
# # Trim/discard support测试

class argparse_operator:
    def __init__(self):
        self.parser = argparse.ArgumentParser(prog='argparse')
        self.setup_parse()

    def setup_parse(self):
        sub_parser = self.parser.add_subparsers()

        self.parser.add_argument('-v',
                                 '--version',
                                 dest='version',
                                 help='Show current version',
                                 action='store_true')

        parser_basic = sub_parser.add_parser("basic",aliases=['b'],help='basic test')
        parser_core = sub_parser.add_parser("core",aliases=['c'],help='core test')

        self.parser_basic = parser_basic
        sub_parser_basic = parser_basic.add_subparsers()

        parser_basic_drbd = sub_parser_basic.add_parser("drbd",aliases=['d'],help='DRBD test')
        sub_parser_basic_drbd = parser_basic_drbd.add_subparsers()
        parser_basic_drbd_disk = sub_parser_basic_drbd.add_parser("disk",aliases=['d'],help='Creating drbd resources using a disk')
        parser_basic_drbd_partition = sub_parser_basic_drbd.add_parser("partition",aliases=['p'],help='Creating drbd resources using disk partitions')
        parser_basic_drbd_status = sub_parser_basic_drbd.add_parser("status",aliases=['s'],help='Check the status and configuration of drbd resources')

        parser_basic_linstor = sub_parser_basic.add_parser("linstor",aliases=['l'],help='LINSTOR test')
        sub_parser_basic_linstor = parser_basic_linstor.add_subparsers()
        parser_basic_linstor_node = sub_parser_basic_linstor.add_parser("node",aliases=['n'],help='linstor node test')
        sub_parser_basic_linstor_node = parser_basic_linstor_node.add_subparsers()
        parser_basic_linstor_node_create = sub_parser_basic_linstor_node.add_parser("create",aliases=['c'],help='linstor node create test')
        parser_basic_linstor_node_delete = sub_parser_basic_linstor_node.add_parser("delete",aliases=['d'],help='linstor node delete test')
        parser_basic_linstor_storagepool = sub_parser_basic_linstor.add_parser("storagepool",aliases=['sp'],help='linstor storagepool test')
        sub_parser_basic_linstor_storagepool = parser_basic_linstor_storagepool.add_subparsers()
        parser_basic_linstor_storagepool_lvm = sub_parser_basic_linstor_storagepool.add_parser("lvm",aliases=['l'],help='linstor storagepool lvm test')
        sub_parser_basic_linstor_storagepool_lvm = parser_basic_linstor_storagepool_lvm.add_subparsers()
        parser_basic_linstor_storagepool_lvm_create = sub_parser_basic_linstor_storagepool_lvm.add_parser("create",aliases=['c'],help='linstor storagepool lvm create test')
        parser_basic_linstor_storagepool_lvm_delete = sub_parser_basic_linstor_storagepool_lvm.add_parser("delete",aliases=['d'],help='linstor storagepool lvm delete test')
        parser_basic_linstor_storagepool_thinlvm = sub_parser_basic_linstor_storagepool.add_parser("thinlvm",aliases=['tl'],help='linstor storagepool thinlvm test')
        sub_parser_basic_linstor_storagepool_thinlvm = parser_basic_linstor_storagepool_thinlvm.add_subparsers()
        parser_basic_linstor_storagepool_thinlvm_create = sub_parser_basic_linstor_storagepool_thinlvm.add_parser("create",aliases=['c'],help='linstor storagepool thinlvm create test')
        parser_basic_linstor_storagepool_thinlvm_delete = sub_parser_basic_linstor_storagepool_thinlvm.add_parser("delete",aliases=['d'],help='linstor storagepool thinlvm delete test')
        parser_basic_linstor_storagepool_thinlvm_increase = sub_parser_basic_linstor_storagepool_thinlvm.add_parser("increase",aliases=['i'],help='linstor storagepool thinlvm increase test')
        parser_basic_linstor_storagepool_thinlvm_decrease = sub_parser_basic_linstor_storagepool_thinlvm.add_parser("decrease",aliases=['de'],help='linstor storagepool thinlvm decrease test')
        parser_basic_linstor_resource = sub_parser_basic_linstor.add_parser("resource",aliases=['r'],help='linstor storagepool resource test')
        sub_parser_basic_linstor_resource = parser_basic_linstor_resource.add_subparsers()
        parser_basic_linstor_resource_create = sub_parser_basic_linstor_resource.add_parser("create",aliases=['c'],help='linstor storagepool resource create test')
        parser_basic_linstor_resource_delete = sub_parser_basic_linstor_resource.add_parser("delete",aliases=['d'],help='linstor storagepool resource delete test')
        parser_basic_linstor_resource_promotion = sub_parser_basic_linstor_resource.add_parser("promotion",aliases=['p'],help='linstor storagepool resource promotion test')
        parser_basic_linstor_resource_eviction = sub_parser_basic_linstor_resource.add_parser("eviction",aliases=['e'],help='linstor storagepool resource eviction test')
        parser_basic_linstor_resource_increase = sub_parser_basic_linstor_resource.add_parser("increase",aliases=['i'],help='linstor storagepool resource increase test')
        parser_basic_linstor_resource_decrease = sub_parser_basic_linstor_resource.add_parser("decrease",aliases=['de'],help='linstor storagepool resource decrease test')
        parser_basic_linstor_report = sub_parser_basic_linstor.add_parser("report",aliases=['rp'],help='linstor report test')
        sub_parser_basic_linstor_report = parser_basic_linstor_report.add_subparsers()
        parser_basic_linstor_report_error = sub_parser_basic_linstor_report.add_parser("error",aliases=['e'],help='linstor error-report test')
        parser_basic_linstor_report_sos = sub_parser_basic_linstor_report.add_parser("sos",aliases=['s'],help='linstor sos-report test')
        parser_basic_linstor_responsetime = sub_parser_basic_linstor.add_parser("responsetime",aliases=['rs'],help='linstor response time test')

        parser_basic_trim = sub_parser_basic.add_parser("trim",aliases=['t'],help='trim/discard support test')

        self.parser.set_defaults(func=self.main_usage)
        parser_basic.set_defaults(func=self.basic_operation)
        parser_core.set_defaults(func=self.core_operation)
        parser_basic_drbd.set_defaults(func=self.basic_drbd_operation)
        parser_basic_drbd_disk.set_defaults(func=self.basic_drbd_disk_operation)
        parser_basic_drbd_partition.set_defaults(func=self.basic_drbd_partition_operation)
        parser_basic_drbd_status.set_defaults(func=self.basic_drbd_status_operation)
        parser_basic_linstor.set_defaults(func=self.basic_linstor_operation)
        parser_basic_linstor_node_create.set_defaults(func=self.basic_linstor_node_create_operation)
        parser_basic_linstor_node_delete.set_defaults(func=self.basic_linstor_node_delete_operation)
        parser_basic_linstor_storagepool.set_defaults(func=self.basic_linstor_storagepool_operation)
        parser_basic_linstor_storagepool_lvm.set_defaults(func=self.basic_linstor_storagepool_lvm_operation)
        parser_basic_linstor_storagepool_lvm_create.set_defaults(func=self.basic_linstor_storagepool_lvm_create_operation)
        parser_basic_linstor_storagepool_lvm_delete.set_defaults(func=self.basic_linstor_storagepool_lvm_delete_operation)
        parser_basic_linstor_storagepool_thinlvm.set_defaults(func=self.basic_linstor_storagepool_thinlvm_operation)
        parser_basic_linstor_storagepool_thinlvm_create.set_defaults(func=self.basic_linstor_storagepool_thinlvm_create_operation)
        parser_basic_linstor_storagepool_thinlvm_delete.set_defaults(func=self.basic_linstor_storagepool_thinlvm_delete_operation)
        parser_basic_linstor_storagepool_thinlvm_increase.set_defaults(func=self.basic_linstor_storagepool_thinlvm_increase_operation)
        parser_basic_linstor_storagepool_thinlvm_decrease.set_defaults(func=self.basic_linstor_storagepool_thinlvm_decrease_operation)
        parser_basic_linstor_resource.set_defaults(func=self.basic_linstor_resource_operation)
        parser_basic_linstor_resource_create.set_defaults(func=self.basic_linstor_resource_create_operation)
        parser_basic_linstor_resource_delete.set_defaults(func=self.basic_linstor_resource_delete_operation)
        parser_basic_linstor_resource_promotion.set_defaults(func=self.basic_linstor_resource_promotion_operation)
        parser_basic_linstor_resource_eviction.set_defaults(func=self.basic_linstor_resource_eviction_operation)
        parser_basic_linstor_resource_increase.set_defaults(func=self.basic_linstor_resource_increase_operation)
        parser_basic_linstor_resource_decrease.set_defaults(func=self.basic_linstor_resource_decrease_operation)
        parser_basic_linstor_report.set_defaults(func=self.basic_linstor_report_operation)
        parser_basic_linstor_report_error.set_defaults(func=self.basic_linstor_report_error_operation)
        parser_basic_linstor_report_sos.set_defaults(func=self.basic_linstor_report_sos_operation)
        parser_basic_linstor_responsetime.set_defaults(func=self.basic_linstor_responsetime_operation)
        parser_basic_trim.set_defaults(func=self.basic_trim)

    def perform_all_tests(self,args):
        print("this is 'python3 main.py'")

    def main_usage(self,args):
        if args.version:
            print(f'Version: ？')
        else:
            self.perform_all_tests(args)

    def basic_operation(self,args):
        basic.basic_test.main()

    def core_operation(self,args):
        print("python3 main.py core")

    def basic_drbd_operation(self,args):
        print("python3 main.py basic drbd")

    def basic_drbd_disk_operation(self,args):
        basic.drbd_disk.main()

    def basic_drbd_partition_operation(self,args):
        basic.drbd_partition.main()

    def basic_drbd_status_operation(self,args):
        basic.drbd_status.main()

    def basic_linstor_operation(self,args):
        print("python3 main.py basic linstor")

    def basic_linstor_node_operation(self,args):
        print("python3 main.py basic linstor node")

    def basic_linstor_node_create_operation(self,args):
        basic.linstor_node_create.main()

    def basic_linstor_node_delete_operation(self,args):
        basic.linstor_node_delete.main()

    def basic_linstor_storagepool_operation(self,args):
        print("python3 main.py basic linstor storagepool")

    def basic_linstor_storagepool_lvm_operation(self,args):
        print("python3 main.py basic linstor storagepool lvm")

    def basic_linstor_storagepool_lvm_create_operation(self,args):
        basic.linstor_sp_lvm_create.main()

    def basic_linstor_storagepool_lvm_delete_operation(self,args):
        basic.linstor_sp_lvm_delete.main()

    def basic_linstor_storagepool_thinlvm_operation(self,args):
        print("python3 main.py basic linstor storagepool thinlvm")

    def basic_linstor_storagepool_thinlvm_create_operation(self,args):
        basic.linstor_sp_thinlvm_create.main()

    def basic_linstor_storagepool_thinlvm_delete_operation(self,args):
        basic.linstor_sp_thinlvm_delete.main()

    def basic_linstor_storagepool_thinlvm_increase_operation(self,args):
        basic.linstor_sp_thinlvm_increase.main()

    def basic_linstor_storagepool_thinlvm_decrease_operation(self,args):
        print("python3 main.py basic linstor storagepool thinlvm decrease")

    def basic_linstor_resource_operation(self,args):
        print("python3 main.py basic linstor resource")

    def basic_linstor_resource_create_operation(self,args):
        basic.linstor_r_create.main()

    def basic_linstor_resource_delete_operation(self,args):
        basic.linstor_r_delete.main()

    def basic_linstor_resource_promotion_operation(self,args):
        basic.linstor_r_promotion.main()

    def basic_linstor_resource_eviction_operation(self,args):
        basic.linstor_r_eviction.main()

    def basic_linstor_resource_increase_operation(self,args):
        basic.linstor_r_increase.main()

    def basic_linstor_resource_decrease_operation(self,args):
        basic.linstor_r_decrease.main()

    def basic_linstor_report_operation(self,args):
        print("python3 main.py basic linstor report")

    def basic_linstor_report_error_operation(self,args):
        basic.linstor_report_error.main()

    def basic_linstor_report_sos_operation(self,args):
        basic.linstor_report_sos.main()

    def basic_linstor_responsetime_operation(self,args):
        basic.linstor_responsetime.main()

    def basic_trim(self,args):
        basic.trim.main()


    def parser_init(self):
        args = self.parser.parse_args()
        args.func(args)


if __name__ == "__main__":
    cmd = argparse_operator()
    cmd.parser_init()

