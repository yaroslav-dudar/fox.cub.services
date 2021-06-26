import httpx
import asyncio

SOURCE = (
    "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt"
)


async def testProxy(proxy_addr: str) -> None:
    proxies = httpx.Proxy(
        url=f"http://{proxy_addr}",
        mode="TUNNEL_ONLY",
    )

    async with httpx.AsyncClient(proxies=proxies) as client:
        try:
            _ = await client.request("GET", "https://google.com")
        except Exception:
            pass
        else:
            print(proxy_addr)


proxy_list = httpx.get(SOURCE).text
tasks = [testProxy(ip) for ip in proxy_list.splitlines()[:100]]


async def main() -> None:
    await asyncio.gather(*tasks)


asyncio.run(main())
