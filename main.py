import logging
import random

from fastapi import FastAPI, Request
from faker import Faker
from io import BytesIO
from PIL import Image
from fastapi.responses import StreamingResponse
from datetime import datetime, timedelta
from fastapi_utils.tasks import repeat_every
import time
import asyncio
import httpx
from fastapi.middleware.cors import CORSMiddleware
import base64
from hashlib import md5
import json
from fastapi.staticfiles import StaticFiles

# Set logging level to WARNING to suppress INFO messages
logging.basicConfig(level=logging.WARNING)

app = FastAPI()
fake = Faker()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有来源
    allow_credentials=True,
    allow_methods=["*"],  # 允许所有方法
    allow_headers=["*"],  # 允许所有头
)


recorded_ips = {}

def generate_image(traceId):
    seed = int(md5(str(traceId).encode()).hexdigest(), 16) % (10 ** 8)
    random.seed(seed)
    image = Image.new('RGB', (50, 50), color=(random.randint(0, 255), random.randint(0, 255), random.randint(0, 255)))
    return image

def generate_base64_image(traceId):
    # 使用traceId生成一个固定种子
    seed = int(md5(str(traceId).encode()).hexdigest(), 16) % (10 ** 8)
    random.seed(seed)

    image = Image.new('RGB', (50, 50), color=(random.randint(0, 255), random.randint(0, 255), random.randint(0, 255)))
    buffer = BytesIO()
    image.save(buffer, format="WEBP")
    buffer.seek(0)
    img_str = base64.b64encode(buffer.getvalue()).decode('utf-8')
    return img_str


@app.get("/trace/openai")
async def openai_request(url: str, key: str, model: str = "gpt-4o", stream: bool = False):
    global recorded_ips
    traceId = int(time.time())

    current_time = datetime.now()

    # 判断recorded_ips是否有traceId,如果没有，则新建一个set
    if traceId not in recorded_ips:
        recorded_ips[traceId] = (current_time, [], [])

    # 立即返回时间戳
    response = {"traceId": traceId, "image": generate_base64_image(traceId)}

    # 异步发送 POST 请求
    asyncio.create_task(send_post_request(url, key, model, traceId, stream))

    return response


async def send_post_request(url: str, key: str, model: str, traceId: str, stream: bool):
    global recorded_ips
    headers = {
        'Accept': '',
        'User-Agent': 'Apifox/1.0.0 (https://apifox.com)',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {key}'
    }
    # 改成你的图片地址
    image_url = f"https://example.com/trace/fake-image?traceId={traceId}"
    data = {
        "model": model,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": image_url}},
                    {"type": "text", "text": "What is this?"}
                ]
            }
        ],
        "max_tokens": 50,
        "stream": stream
    }

    async with httpx.AsyncClient(timeout=httpx.Timeout(120.0)) as client:
        try:
            if stream:
                # 流式请求：逐行读取 SSE
                chunk_count = 0
                full_content = ""
                async with client.stream("POST", url, headers=headers, json=data) as response:
                    recorded_ips[traceId][2].append(f"渠道回复状态码: {response.status_code}")
                    if response.status_code != 200:
                        body = await response.aread()
                        recorded_ips[traceId][2].append(f"Error: {body.decode('utf-8', errors='replace')}")
                    else:
                        async for line in response.aiter_lines():
                            if line.startswith("data: "):
                                chunk_count += 1
                                data_str = line[6:]
                                if data_str == "[DONE]":
                                    break
                                try:
                                    chunk = json.loads(data_str)
                                    delta = chunk.get("choices", [{}])[0].get("delta", {}).get("content", "")
                                    full_content += delta
                                except (json.JSONDecodeError, KeyError, IndexError):
                                    pass
                        recorded_ips[traceId][2].append(f"流式响应完成，共 {chunk_count} chunks，内容: {full_content[:200]}")
                        recorded_ips[traceId][2].append(f"完成探测")
            else:
                # 非流式请求（保持原有逻辑）
                response = await client.post(url, headers=headers, json=data)
                recorded_ips[traceId][2].append(f"渠道回复： {response.text}")
                print("渠道回复 ", response.text)
                if response.status_code != 200:
                    recorded_ips[traceId][2].append(f"Error: {response.text}")
                else:
                    recorded_ips[traceId][2].append(f"完成探测")
        except Exception as e:
            # 输出异常信息
            recorded_ips[traceId][2].append(f"Exception: {str(e)}")

    return traceId


@app.on_event("startup")
@repeat_every(seconds=60)  # Run every 60 seconds
def cleanup_old_ips():
    global recorded_ips
    current_time = datetime.now()
    for traceId in list(recorded_ips.keys()):
        timestamp, _, _ = recorded_ips[traceId]
        if current_time - timestamp > timedelta(minutes=3):
            del recorded_ips[traceId]


@app.get("/trace/get-agent")
async def fake_image(request: Request, traceId: str):
    global recorded_ips
    traceId = int(traceId)
    # print("traceId ", traceId)
    # print("recorded_ips ", recorded_ips)
    if traceId in recorded_ips:
        # print("====== recorded_ips[traceId][2] ", recorded_ips[traceId][2])
        res = recorded_ips[traceId][2]
        current_time = datetime.now()
        timestamp, _, _ = recorded_ips[traceId]
        if current_time - timestamp > timedelta(seconds=80):
            current_time = datetime.now()
            time_str = current_time.strftime("%H:%M:%S")
            recorded_ips[traceId][2].append(str(time_str) + " 超过80秒未收到响应，未收到回调，可能来自逆向，完成探测" )
            return recorded_ips[traceId][2]
        else:
            return recorded_ips[traceId][2]
    return set()


@app.get("/trace/fake-image")
async def fake_image(request: Request, traceId: str):
    global recorded_ips
    current_time = datetime.now()
    traceId = int(traceId)
    # 判断recorded_ips是否有traceId,如果没有，则新建一个set
    if traceId not in recorded_ips:
        recorded_ips[traceId] = (current_time, [], [])

    # 生成一个假的 WebP 图片
    image = generate_image(traceId)
    buffer = BytesIO()
    image.save(buffer, format="WEBP")
    buffer.seek(0)

    # 获取请求的 host, 源 IP, user agent 和其他详细信息
    user_agent = request.headers.get('user-agent')
    if user_agent and "IPS" in user_agent:
        user_agent = "Azure " + user_agent
    if user_agent and "OpenAI" in user_agent:
        user_agent = user_agent
    if user_agent is None:
        user_agent = "未知UA"
    x_forwarded_for = request.headers.get('x-forwarded-for')
    cf_connecting_ip = request.headers.get('cf-connecting-ip')
    client_host = request.client.host
    headers = request.headers

    # 给header 脱敏，将header中所有的ip的中间部分用*代替

    # 检查并记录IP地址
    new_ips = True
    # if cf_connecting_ip and cf_connecting_ip in recorded_ips[traceId][1]:
    #     new_ips = False
    # else:
    #     recorded_ips[traceId][1].append(cf_connecting_ip)

    # if x_forwarded_for:
    #     for ip in x_forwarded_for.split(','):
    #         ip = ip.strip()
    #         if ip in recorded_ips[traceId][1]:
    #             new_ips = False
    #         else:
    #             recorded_ips[traceId][1].append(ip)
    #         break

    if new_ips:
        # x_forwarded_for脱敏，ip的中间部分用*代替
        new_x_forwarded_for = ""
        if x_forwarded_for:
            for ip in x_forwarded_for.split(','):
                ip_parts = ip.split('.')
                if len(ip_parts) == 4:
                    new_x_forwarded_for = new_x_forwarded_for +  f"{ip_parts[0]}.***.***.{ip_parts[3]}, "
                    # break

        # new_cf_connecting_ip = ""
        # if cf_connecting_ip:
        #     cf_connecting_ip_parts = cf_connecting_ip.split('.')
        #     if len(cf_connecting_ip_parts) == 4:
        #         cf_connecting_ip = f"{cf_connecting_ip_parts[0]}.***.***.{cf_connecting_ip_parts[3]}"

        cf_ipcountry = request.headers.get('cf-ipcountry')
        # x_openai_originator = request.headers.get('x-openai-originator')
        time_str = current_time.strftime("%H:%M:%S")
        if user_agent is None:
            recorded_ips[traceId][2].append(str(time_str) + " " + user_agent + "  x_forwarded_for：" + str(
                new_x_forwarded_for) + "  cf_connecting_ip：" + str(cf_connecting_ip)
                                            + "  cf_ipcountry：" + str(cf_ipcountry) + "  详细请求头信息：" + str(headers))
        else:
            recorded_ips[traceId][2].append(str(time_str) + " " + user_agent + "  x_forwarded_for：" + str(new_x_forwarded_for) + "  cf_connecting_ip：" + str(cf_connecting_ip)
                                        + "  cf_ipcountry：" + str(cf_ipcountry))
        print(
            f"Time: {current_time}, TraceId: {traceId}, x_forwarded_for: {x_forwarded_for}, cf_connecting_ip: {cf_connecting_ip}, Client Host: {client_host}, User Agent: {user_agent}")

    return StreamingResponse(buffer, media_type="image/webp")


# Mount static files directory for frontend
app.mount("/", StaticFiles(directory="static", html=True), name="static")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8921, log_level="warning")