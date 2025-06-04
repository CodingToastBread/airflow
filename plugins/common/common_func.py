def get_sftp():
    print('sftp 작업을 시작합니다.')

def regist(name, sex, *args):
    print(f"이름: {name}")
    print(f"성별: {sex}")
    print(f"기타 옵션들: {args}")


def regist2(name, sex, *args, **kwargs):
    print(f"이름: {name}")
    print(f"성별: {sex}")
    print(f"기타 옵션들: {args}")
    email = kwargs.get('email')
    phone = kwargs.get("phone")
    if email:
        print(email)
    if phone:
        print(phone)

def good_bye_world(name, age, *args, **kwargs) -> None:
    print('good_by_world')
    print('my name is ...', name)
    print('my age is ...', age)
    print('arg list : ', print(args))
    print('kwargs -> key_sample1 : ', \
        print(kwargs.get('key_sample1') or 'no value found'))
