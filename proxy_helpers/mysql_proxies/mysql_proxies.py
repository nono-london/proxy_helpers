""" Handles queries to MySQL """
from threading import RLock
from typing import Union, Optional

import pandas as pd
from mysql_helpers.mysql_con.mysql_sync import MySQLConnectorNative


class MySQLProxy(MySQLConnectorNative):
    """ MySQL class helpers with Rlock use  """

    def __init__(self, db_host: Optional[str] = None,
                 db_port: Union[int, str] = None,
                 db_user: Optional[str] = None,
                 db_password: Optional[str] = None,
                 db_name: Optional[str] = None,
                 raise_on_warnings: bool = False,
                 proxy_universe_size: int = 100):

        super().__init__(db_host, db_port, db_user, db_password, db_name, raise_on_warnings)
        self.proxy_universe_size: int = proxy_universe_size

        self.mysql_connection_rlock: RLock = RLock()

    def delete_proxy(self, proxy_id: int):
        """ methods that related to proxy and handle there deletion with their ID"""
        sql_string = """
                            DELETE FROM tbl_proxy_url
                            WHERE proxy_id= %s
                        """
        return self.execute_one_query(sql_query=sql_string, sql_variables=(proxy_id,))

    def get_proxy_universe(self, proxy_universe_size: int = 1000,
                           return_as_list_of_dicts: bool = False,
                           shuffle_results: bool = True) -> Union[pd.DataFrame, list]:
        """ methods that return a datagrame of proxies """
        sql_string = """
                    SELECT CONCAT(a.proxy_url,':', a.proxy_port) as 'full_url',
                        a.* 
                    FROM tbl_proxy_url a 
                    ORDER BY a.error_count ASC 
                    LIMIT %s
                """
        result_df = self.fetch_all_as_df(sql_query=sql_string,
                                         sql_variables=(proxy_universe_size,))
        if shuffle_results:
            result_df = result_df.sample(n=len(result_df))
        if return_as_list_of_dicts:
            return result_df.to_dict(orient='records')
        return result_df

    def update_proxy_score(self, proxy_id: Union[str, int], success: bool):
        """ method that update the proxy score"""
        proxy_id:int = int(proxy_id)

        if success:
            sql_string = """
                            UPDATE tbl_proxy_url 
                            SET error_count= GREATEST((error_count - 1), -5000)
                            WHERE proxy_id= %s
                        """
        else:
            sql_string = """
                            UPDATE tbl_proxy_url  
                            SET error_count= LEAST((error_count + 1), 5000)
                            WHERE proxy_id= %s
                        """
        sql_variables:tuple = (proxy_id,)
        query_result: int = self.execute_one_query(sql_query=sql_string,
                                                   sql_variables=sql_variables,
                                                   close_connection=True)

        return query_result

    def update_proxy_selenium_score(self, proxy_id: Union[str, int], success: bool):
        """ methods that updates proxy score for selenium"""
        proxy_id:int = int(proxy_id)

        if success:
            sql_string = """
                        UPDATE `tbl_proxy_url`
                        SET
                        `selenium_success` = `selenium_success` + 1
                        WHERE `proxy_id` =  %s
                        """
        else:
            sql_string = """
                        UPDATE `tbl_proxy_url`
                        SET
                        `selenium_success` = `selenium_success` - 1
                        WHERE `proxy_id` =  %s
                        """
        sql_variables:tuple=(proxy_id,)
        query_result: int = self.execute_one_query(sql_query=sql_string,
                                                   sql_variables=sql_variables)

        return query_result

    def refresh_generator_proxy_dict(self, proxy_universe_size=100):
        """ method that creates a generator of proxies"""
        proxy_list: list = self.get_proxy_universe(proxy_universe_size=proxy_universe_size,
                                                   return_as_list_of_dicts=True,
                                                   shuffle_results=True,
                                                   )
        if proxy_list is None:
            return None
        for proxy in proxy_list:
            yield proxy


class ProxyHandler(MySQLProxy):
    """ Class proxy """

    def __init__(self,
                 proxy_universe_size: int = 100):
        super().__init__(proxy_universe_size=proxy_universe_size)
        # Proxy generator
        self.proxy_yield_rlock: RLock = RLock()
        self.next_proxy_yield_rlock: RLock = RLock()
        self.print_proxy_rlock: RLock = RLock()
        self.proxy_generator = None

        # Initiate variables
        self._set_proxy_generator(proxy_universe_size=self.proxy_universe_size)

    def _set_proxy_generator(self, proxy_universe_size: Optional[int] = 100):
        with self.proxy_yield_rlock:
            self.proxy_yield = self.refresh_generator_proxy_dict(
                proxy_universe_size=proxy_universe_size)

    def _print(self, str_to_print: object,
               verbose: bool = False):
        with self.print_proxy_rlock:
            if verbose:
                str_to_print = str(str_to_print)
                print(str_to_print)
            else:
                print(".", end="")

    @staticmethod
    def get_requests_proxies_as_dict(full_url: str) -> dict:
        """ return dict {http:full_proxy_url, https:full_proxy_url using a full_proxy_url}"""
        return {'http': full_url, 'https': full_url}

    def get_next_proxy_from_generator(self):
        """ Return a generator of proxy dicts which include country, score and else.
            At stop iteration, the generator is refreshed"""
        with self.next_proxy_yield_rlock:
            while True:
                try:
                    return next(self.proxy_yield)
                except StopIteration:
                    # self._print(
                    #     f"Error while getting next proxy: {'StopIteration'}")
                    self._set_proxy_generator()


if __name__ == '__main__':
    from dotenv import load_dotenv

    load_dotenv()
    my_getter = MySQLProxy()
    proxy_sql_query = """
    SELECT * FROM tbl_proxy_url ORDER BY error_count DESC LIMIT 10 
    """
    print(my_getter.fetch_all_as_df(sql_query=proxy_sql_query))
    my_proxy = ProxyHandler()
    counter: int = 1
    while counter < 10:
        proxy_dict = my_proxy.get_next_proxy_from_generator()
        print(proxy_dict['full_url'], proxy_dict['proxy_country'])
        counter += 1
