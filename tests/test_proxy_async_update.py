from datetime import datetime

import pytest
from dotenv import load_dotenv

from proxy_helpers.mysql_proxies.mysql_proxies_async import ProxyHandler

load_dotenv()


@pytest.mark.asyncio
async def test_update_proxy_score():
    """create a fake proxy, update error_count and error_selenium_count
        then delete it
    """
    proxy_handler = ProxyHandler(proxy_universe_size=10)
    proxy_web_name = "test_async"
    proxy_dict = {"upload_datetime": datetime.utcnow(),
                  "proxy_web_name": proxy_web_name,
                  "proxy_url": proxy_web_name,
                  "proxy_port": 1,
                  "proxy_country": "",
                  "proxy_town": "",
                  "proxy_speed": 0
                  }

    await proxy_handler.insert_proxy(proxy_dict=proxy_dict)

    sql_query = """SELECT * FROM tbl_proxy_url WHERE proxy_web_name=%s"""
    proxy_dict_df = await proxy_handler.fetch_all_as_df(sql_query=sql_query, sql_variables=(proxy_web_name,))
    print(proxy_dict_df)
    for index, proxy_dict in proxy_dict_df.iterrows():
        await proxy_handler.update_proxy_score(success=True, proxy_id=proxy_dict['proxy_id'])
        await proxy_handler.update_proxy_score(success=False, proxy_id=proxy_dict['proxy_id'])
        await proxy_handler.update_proxy_selenium_score(success=False, proxy_id=proxy_dict['proxy_id'])
        await proxy_handler.update_proxy_selenium_score(success=False, proxy_id=proxy_dict['proxy_id'])
        await proxy_handler.delete_proxy(proxy_id=proxy_dict['proxy_id'])
