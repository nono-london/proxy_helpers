from dotenv import load_dotenv

from proxy_helpers.mysql_proxies.mysql_proxies import MySQLProxy, ProxyHandler

load_dotenv()


def test_mysql_proxies():
    my_getter = MySQLProxy()
    proxy_sql_query = """
    SELECT * FROM tbl_proxy_url ORDER BY error_count DESC LIMIT 10 
    """
    result_df = my_getter.fetch_all_as_df(sql_query=proxy_sql_query)
    assert result_df is not None


def test_new_proxies():
    proxy_number: int = 5

    my_proxy = ProxyHandler()
    while proxy_number > 0:
        proxy_dict = my_proxy.get_next_proxy_from_generator()
        print(proxy_dict["full_url"], proxy_dict["proxy_country"])
        proxy_number -= 1


if __name__ == "__main__":
    test_mysql_proxies()
    test_new_proxies()
