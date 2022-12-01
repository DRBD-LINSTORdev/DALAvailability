import argparse
import manage
import controller

# python3 main.py
# # 一键执行全部测试
# python3 main.py  manage(m)
# # 管理操作测试
# python3 main.py func(f)
# # 功能测试
# python3 main.py func(f) primary(pry)
# # Single Primary
# python3 main.py func(f) promotion(pro) mount(m)
# python3 main.py func(f) promotion(pro) dd
# python3 main.py func(f) promotion(pro)
# # Automatic promotion
# python3 main.py func(f) consistency(c)
# # 数据一致性
# python3 main.py func(f) sync(s)
# # 数据同步
# python3 main.py func(f) quorum(q) target01(t01)
# # Quorum&tiebreaker 目标1
# python3 main.py func(f) quorum(q) target01(t02)
# # Quorum&tiebreaker 目标2
# python3 main.py func(f) quorum(q) target01(t03)
# # Quorum&tiebreaker 目标3
# python3 main.py func(f) status(st)
# # Inconsistent/Outdated
# python3 main.py other(o) response(r)
# # LINSTOR 命令响应时间测试
# python3 main.py other(o) evicition(e)
# # LINSTOR eviction 测试
# python3 main.py other(o) gituple(gi)
# # DRBD GI tuple 测试

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

        parser_manage = sub_parser.add_parser("manage",aliases=['mo'],help='manage operation test')

        parser_func = sub_parser.add_parser("func",aliases=['f'],help='function test')
        self.parser_func = parser_func
        sub_parser_func = parser_func.add_subparsers()
        parser_func_primary = sub_parser_func.add_parser("primary",aliases=['pry'],help='Single Primary test')

        parser_func_promotion = sub_parser_func.add_parser("promotion",aliases=['pro'],help='Automatic promotion test')
        sub_parser_func_promotion = parser_func_promotion.add_subparsers()
        sub_parser_func_promotion_mount = sub_parser_func_promotion.add_parser("mount",aliases=['m'],help='Single primary mount&umount test')
        sub_parser_func_promotion_dd = sub_parser_func_promotion.add_parser("dd",help='Single primary dd test')
        parser_func_consistency = sub_parser_func.add_parser("consistency",aliases=['c'],help='data consisitency test')
        parser_func_sync = sub_parser_func.add_parser("sync",aliases=['s'],help='data sync test')

        parser_func_quorum = sub_parser_func.add_parser("quorum",aliases=['q'],help='Quorum&tiebreaker test')
        sub_parser_func_quorum = parser_func_quorum.add_subparsers()
        sub_parser_func_quorum_target01 = sub_parser_func_quorum.add_parser("target01",aliases=['t01'],help='Quorum&tiebreaker target01 test')
        sub_parser_func_quorum_target02 = sub_parser_func_quorum.add_parser("target02",aliases=['t02'],help='Quorum&tiebreaker target02 test')
        sub_parser_func_quorum_target03 = sub_parser_func_quorum.add_parser("target03",aliases=['t03'],help='Quorum&tiebreaker target03 test')

        parser_func_status = sub_parser_func.add_parser("status",aliases=['st'],help='Inconsistent/Outdated test')

        parser_other = sub_parser.add_parser("other",aliases=['o'],help='other test')
        self.parser_other =parser_other
        sub_parser_other = parser_other.add_subparsers()
        sub_parser_other_response = sub_parser_other.add_parser("response",aliases=['r'],help='linstor command response time test')
        sub_parser_other_evicition = sub_parser_other.add_parser("evicition",aliases=['e'],help='linstor evicition test')
        sub_parser_other_gituple = sub_parser_other.add_parser("gituple",aliases=['gi'],help='linstor gituple test')

        self.parser.set_defaults(func=self.main_usage)
        parser_manage.set_defaults(func=self.manage_operation)
        self.parser_func.set_defaults(func=self.func_operation)
        parser_func_primary.set_defaults(func=self.func_primary_operation)
        sub_parser_func_promotion_mount.set_defaults(func=self.func_promotion_mount_operation)
        sub_parser_func_promotion_dd.set_defaults(func=self.func_promotion_dd_operation)
        parser_func_promotion.set_defaults(func=self.func_promotion_operation)
        parser_func_consistency.set_defaults(fun=self.func_consistency_operation)
        parser_func_sync.set_defaults(func=self.func_sync_operation)
        sub_parser_func_quorum_target01.set_defaults(func=self.func_quorum_target01_operation)
        sub_parser_func_quorum_target02.set_defaults(func=self.func_quorum_target02_operation)
        sub_parser_func_quorum_target03.set_defaults(func=self.func_quorum_target03_operation)
        parser_func_status.set_defaults(func=self.func_status_operation)
        parser_other.set_defaults(func=self.other_operation)
        sub_parser_other_response.set_defaults(func=self.other_response_operation)
        sub_parser_other_evicition.set_defaults(func=self.other_evicition_operation)
        sub_parser_other_gituple.set_defaults(func=self.other_gituple_operation)

    def perform_all_tests(self,args):
        print("this is 'python3 main.py'")

    def main_usage(self,args):
        if args.version:
            print(f'Version: ？')
        else:
            self.perform_all_tests(args)

    def manage_operation(self,args):
        manage.main()

    def func_operation(self,args):
        self.parser_func.print_help()

    def func_primary_operation(self,args):
        print("this is 'python3 main.py func primary'")

    def func_promotion_mount_operation(self,args):
        print("this is 'python3 main.py func promotion mount'")

    def func_promotion_dd_operation(self,args):
        print("this is 'python3 main.py func promotion dd'")

    def func_promotion_operation(self,args):
        print("this is 'python3 main.py func promotion'")

    def func_consistency_operation(self,args):
        print("this is 'python3 main.py func consistency'")

    def func_sync_operation(self,args):
        print("this is 'python3 main.py func sync'")

    def func_quorum_target01_operation(self,args):
        controller.QuorumTest().main()

    def func_quorum_target02_operation(self,args):
        print("this is 'python3 main.py func quorum target02'")

    def func_quorum_target03_operation(self,args):
        print("this is 'python3 main.py func quorum target03'")

    def func_status_operation(self,args):
        controller.inconsistent_test.main()

    def other_operation(self,args):
        print("this is 'python3 main.py other'")
        print(self.parser_other.print_help())

    def other_response_operation(self,args):
        controller.LinstorCommandResponse().main()

    def other_evicition_operation(self,args):
        controller.LinstorEviction().main()

    def other_gituple_operation(self,args):
        print("this is 'python3 main.py other gituple'")

    def parser_init(self):
        args = self.parser.parse_args()
        args.func(args)


if __name__ == "__main__":
    cmd = argparse_operator()
    cmd.parser_init()

