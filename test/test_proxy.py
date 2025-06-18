import requests
import random
import os
from dotenv import load_dotenv

load_dotenv()

raw_proxies = os.getenv("CUSTOM_PROXIES_CARLIST", "")
proxies_list = []

if raw_proxies:
    proxy_entries = raw_proxies.split(",")
    for entry in proxy_entries:
        ip, port, user, pwd = entry.strip().split(":")
        proxy_url = f"http://{user}:{pwd}@{ip}:{port}"
        proxies_list.append(proxy_url)

proxy = random.choice(proxies_list)
proxies = {
    "http": proxy,
    "https": proxy
}

response = requests.get("http://ipinfo.io/ip", proxies=proxies, timeout=20)
print("IP kamu:", response.text.strip())
