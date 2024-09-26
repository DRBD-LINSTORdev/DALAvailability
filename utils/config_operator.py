import re

def ip_format(ip_addr):
    pattern = re.compile(r'((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})(\.((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})){3}')
    result = pattern.match(ip_addr)
    if result:
        print("true")
        return True
    else:
        print("false")
        return False