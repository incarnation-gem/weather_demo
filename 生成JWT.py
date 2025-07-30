#!/usr/bin/env python3
import sys
import time
import jwt
from datetime import datetime, timedelta

# Open PEM
private_key = """-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIHioY4NMxin5qZ8D18i296EMTZ2VB5kp+jgkdNjKm5rb
-----END PRIVATE KEY-----"""

def generate_jwt(hours=10):
    """生成JWT Token
    
    Args:
        hours (int): Token有效期小时数，默认10小时
    
    Returns:
        str: 编码后的JWT Token
    """
    now = int(time.time())
    expire_seconds = hours * 3600
    
    payload = {
        'iat': now - 30,  # 签发时间（略微提前30秒防止时钟偏差）
        'exp': now + expire_seconds,  # 过期时间
        'sub': '46KWJQQATC'
    }
    headers = {
        'kid': 'KBB6BQNHM5'
    }
    
    # Generate JWT
    encoded_jwt = jwt.encode(payload, private_key, algorithm='EdDSA', headers=headers)
    
    # 计算过期时间
    expire_time = datetime.fromtimestamp(now + expire_seconds)
    
    print(f"JWT Token生成成功:")
    print(f"  Token: {encoded_jwt}")
    print(f"  有效期: {hours}小时")
    print(f"  过期时间: {expire_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    return encoded_jwt

def check_jwt_expiry(token):
    """检查JWT Token是否过期
    
    Args:
        token (str): JWT Token
        
    Returns:
        tuple: (是否过期, 剩余时间秒数)
    """
    try:
        # 解码JWT不验证签名，只获取payload
        payload = jwt.decode(token, options={"verify_signature": False})
        exp = payload.get('exp')
        
        if exp:
            current_time = int(time.time())
            remaining = exp - current_time
            
            if remaining <= 0:
                return True, 0
            else:
                return False, remaining
    except Exception as e:
        print(f"JWT解析错误: {e}")
        return True, 0
    
    return True, 0

if __name__ == "__main__":
    # 检查命令行参数
    hours = 10  # 默认10小时
    if len(sys.argv) > 1:
        try:
            hours = int(sys.argv[1])
            if hours <= 0:
                print("警告: 有效期必须大于0小时，使用默认值10小时")
                hours = 10
        except ValueError:
            print("警告: 无效的小时数参数，使用默认值10小时")
    
    generate_jwt(hours) 