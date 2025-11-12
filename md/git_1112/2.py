import requests
import json

def test_chat_completion():
    api_key = "sk-jfabmheygmntjcrnesavgbaoekmnbnqgwbpwxksssapykfuc"
    api_base = "https://api.siliconflow.cn/v1"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    data = {
        "model": "Qwen/Qwen2.5-7B-Instruct",
        "messages": [
            {"role": "user", "content": "你好，请简单介绍一下你自己"}
        ],
        "stream": False
    }

    try:
        response = requests.post(f"{api_base}/chat/completions",
                               headers=headers,
                               json=data,
                               timeout=30)
        print(f"状态码: {response.status_code}")

        if response.status_code == 200:
            result = response.json()
            print("✓ 聊天功能测试成功！")
            print(f"回复: {result['choices'][0]['message']['content']}")
            return True
        else:
            print(f"聊天测试失败: {response.text}")
            return False
    except Exception as e:
        print(f"异常: {e}")
        return False

if __name__ == "__main__":
    test_chat_completion()