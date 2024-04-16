import asyncio

import pytest
from dotenv import load_dotenv

from proxy_helpers.mysql_proxies.mysql_proxies_async import MySQLProxy, ProxyHandler

load_dotenv()


@pytest.mark.asyncio
async def test_mysql_proxies():
    my_getter = MySQLProxy()
    proxy_sql_query = """
    SELECT * FROM tbl_proxy_url ORDER BY error_count DESC LIMIT 10 
    """
    result_df = await my_getter.fetch_all_as_df(sql_query=proxy_sql_query)
    assert result_df is not None


@pytest.mark.asyncio
async def test_proxy_generator():
    my_proxy = ProxyHandler()
    proxies = []
    proxy_number: int = 5
    for _ in range(0, 5):
        proxy_dict = await my_proxy.get_next_proxy_from_generator()
        proxies.append(proxy_dict)
        print(proxy_dict["full_url"], proxy_dict["proxy_country"])

    assert len(proxies) == proxy_number


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_mysql_proxies())
    loop.run_until_complete(test_proxy_generator())
