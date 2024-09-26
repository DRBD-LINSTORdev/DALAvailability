import yaml

class ConfFile(object):
    def __init__(self,file_path):
        self.file_path = file_path

    def read_yaml(self):
        """
        读yaml文件
        """
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                yaml_file = yaml.load(f,Loader=yaml.FullLoader)
            return yaml_file
        except FileNotFoundError:
            print("File not found")
        except TypeError:
            print("Error in the type of file .")

    def update_yaml(self,yaml_dict):
        """
        更新yaml文件
        """
        try:
            with open(self.file_path, 'w', encoding='utf-8') as f:
                yaml.dump(yaml_dict, f, default_flow_style=False)
        except FileNotFoundError:
            print("File not found")
        except TypeError:
            print("Error in the type of file .")

