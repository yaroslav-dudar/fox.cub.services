import httpx
import asyncio

SOURCE = 'https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt'

async def testProxy(proxy_addr):
    proxies = {"all://": f"http://{proxy_addr}"}

    async with httpx.AsyncClient(proxies=proxies) as client:
        try:
            r = await client.request("GET", "https://google.com")
        except httpx.ProxyError as e:
            pass
        else:
            print(proxy_addr)



proxy_list = httpx.get(SOURCE).text
tasks = [testProxy(ip) for ip in proxy_list.splitlines()[:100]]


async def main():
    await asyncio.gather(*tasks)

asyncio.run(main())