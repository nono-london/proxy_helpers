from datetime import datetime

from dotenv import load_dotenv

from proxy_helpers.mysql_proxies.mysql_pool_proxies import MySQLProxy

load_dotenv()
test_proxy: dict = {
    "upload_datetime": datetime.utcnow(),
    "proxy_url": "test_proxy_url",
    "proxy_port": 80,
    "proxy_web_name": "test_proxy_name"
}


def test_insert_new_proxy():
    my_proxy = MySQLProxy()
    # delete previous proxy if any
    my_proxy.delete_proxy(
        proxy_url=test_proxy["proxy_url"], proxy_port=test_proxy["proxy_port"]
    )
    result = my_proxy.insert_proxy(test_proxy)
    assert result == 1


def test_success_new_proxy():
    my_proxy = MySQLProxy()
    # delete previous proxy if any
    result = my_proxy.update_proxy_score(
        proxy_url=test_proxy["proxy_url"],
        proxy_port=test_proxy["proxy_port"],
        success=True,
    )
    assert result == 1


def test_error_new_proxy():
    my_proxy = MySQLProxy()
    # delete previous proxy if any
    result = my_proxy.update_proxy_score(
        proxy_url=test_proxy["proxy_url"],
        proxy_port=test_proxy["proxy_port"],
        success=False,
    )
    assert result == 1


def test_delete_proxy():
    my_proxy = MySQLProxy()
    result = my_proxy.delete_proxy(
        proxy_url=test_proxy["proxy_url"], proxy_port=test_proxy["proxy_port"]
    )
    assert result == 1


if __name__ == "__main__":
    test_insert_new_proxy()
    test_success_new_proxy()
    test_error_new_proxy()
    test_delete_proxy()
