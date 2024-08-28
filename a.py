import requests

url = "http://192.168.0.114:1260/omllava/v1/chat/completions"

body = {
        "model": "Qwen2-7B-Instruct",
        "messages": 
            [
                    {
                        "name": "string",
                        "role": "system",
                        "content": "A chat between a curious user and an artificial intelligence assistant. The assistant gives helpful, detailed, and polite answers to the user's questions."
                    },
                    {
                        "name": "string",
                        "role": "user",
                        "content": "你好"
                    }
        ],
        "max_tokens": 512,
        "presence_penalty": 0,
        "frequency_penalty": 0,
        "top_p": 1,
        "temperature": 0.2,
        "stream": True
    }

resp = requests.post(url=url, json=body).json()
print(resp)