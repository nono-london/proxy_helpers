import asyncio
import logging
from pathlib import Path
from typing import Union, Optional, Dict

import pandas as pd
from mysql_helpers.mysql_con.mysql_async import MySQLConnectorNativeAsync as _MySQLConnectorNativeAsync

logger = logging.getLogger(f"proxy_helpers:{Path(__file__).name}")


class MySQLProxy(_MySQLConnectorNativeAsync):
    """MySQL class helpers with Rlock use"""

    def __init__(
            self,
            db_host: Optional[str] = None,
            db_port: Union[int, str] = None,
            db_user: Optional[str] = None,
            db_password: Optional[str] = None,
            db_name: Optional[str] = None,
            raise_on_warnings: bool = False,
            proxy_universe_size: int = 100,
    ):
        super().__init__(
            db_host, db_port, db_user, db_password, db_name, raise_on_warnings
        )
        self.proxy_universe_size: int = proxy_universe_size

        self.mysql_connection_lock: asyncio.Lock()

    async def insert_proxy(self, proxy_dict: Dict):
        sql_query = """
                    INSERT INTO `tbl_proxy_url`
                    (`upload_datetime`, `proxy_url`, `proxy_port`, `proxy_country`, `proxy_town`, `proxy_speed`, `proxy_web_name`)
                    VALUES
                    (%s, %s, %s, %s, %s, %s, %s)
        """
        proxy_dict["proxy_port"] = int(proxy_dict["proxy_port"])
        sql_variables = (
            proxy_dict["upload_datetime"],
            proxy_dict["proxy_url"],
            proxy_dict["proxy_port"],
            proxy_dict.get("proxy_country"),
            proxy_dict.get("proxy_town"),
            proxy_dict.get("proxy_speed"),
            proxy_dict.get("proxy_web_name"),
        )
        return await self.execute_one_query(sql_query=sql_query,
                                            sql_variables=sql_variables,
                                            close_connection=False)

    async def delete_proxy(
            self,
            proxy_id: Optional[Union[str, int]] = None,
            proxy_url: Optional[str] = None,
            proxy_port: Optional[Union[int, str]] = None,
    ):
        """methods that related to proxy and handle there deletion with their ID"""
        if proxy_id:
            proxy_id: int = int(proxy_id)

            sql_string = """
                    DELETE FROM tbl_proxy_url
                    WHERE proxy_id= %s
                        """
            sql_variables = (proxy_id,)
        elif proxy_url and proxy_port:
            sql_string = """
                    DELETE FROM tbl_proxy_url
                    WHERE proxy_url= %s AND proxy_port=%s
                    """
            sql_variables = (proxy_url, proxy_port)
        else:
            return None

        return await self.execute_one_query(sql_query=sql_string,
                                            sql_variables=sql_variables,
                                            close_connection=False)

    async def get_proxy_universe(
            self,
            proxy_universe_size: int = 1000,
            return_as_list_of_dicts: bool = False,
            shuffle_results: bool = True,
    ) -> Union[pd.DataFrame, list, None]:
        """methods that return a dataframe of proxies"""
        sql_string = """
                    SELECT CONCAT(a.proxy_url,':', a.proxy_port) as 'full_url',
                        a.* 
                    FROM tbl_proxy_url a 
                    ORDER BY a.error_count ASC 
                    LIMIT %s
                """
        result_df = await self.fetch_all_as_df(
            sql_query=sql_string,
            sql_variables=(proxy_universe_size,),
            close_connection=False

        )

        if result_df is None or len(result_df) == 0:
            return None

        if shuffle_results:
            result_df = result_df.sample(n=len(result_df))
        if return_as_list_of_dicts:
            return result_df.to_dict(orient="records")
        return result_df

    async def update_proxy_score(
            self,
            success: bool,
            proxy_id: Optional[Union[str, int]] = None,
            proxy_url: Optional[str] = None,
            proxy_port: Optional[Union[int, str]] = None,
    ) -> Union[int, None]:
        """method that update the proxy score"""

        if success:
            if proxy_id:
                proxy_id: int = int(proxy_id)

                sql_string = """
                            UPDATE tbl_proxy_url 
                            SET error_count= GREATEST((error_count - 1), -5000)
                            WHERE proxy_id= %s
                        """
                sql_variables = (proxy_id,)
            elif proxy_url and proxy_port:
                sql_string = """
                                UPDATE tbl_proxy_url 
                                SET error_count= GREATEST((error_count - 1), -5000)
                                WHERE proxy_url= %s AND proxy_port=%s
                            """
                sql_variables = (proxy_url, proxy_port)
            else:
                return None
        else:
            if proxy_id:
                proxy_id: int = int(proxy_id)

                sql_string = """
                            UPDATE tbl_proxy_url 
                            SET error_count= LEAST((error_count + 1), 5000)
                            WHERE proxy_id= %s
                        """
                sql_variables = (proxy_id,)
            elif proxy_url and proxy_port:
                sql_string = """
                                UPDATE tbl_proxy_url 
                            SET error_count= LEAST((error_count + 1), 5000)
                                WHERE proxy_url= %s AND proxy_port=%s
                            """
                sql_variables = (proxy_url, proxy_port)
            else:
                return None

        query_result: int = await self.execute_one_query(
            sql_query=sql_string,
            sql_variables=sql_variables,
            close_connection=False
        )

        return query_result

    async def update_proxy_selenium_score(self, proxy_id: Union[str, int], success: bool):
        """methods that updates proxy score for selenium"""
        proxy_id: int = int(proxy_id)

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
        sql_variables: tuple = (proxy_id,)
        query_result: int = await self.execute_one_query(
            sql_query=sql_string,
            sql_variables=sql_variables,
            close_connection=False
        )

        return query_result

    async def refresh_generator_proxy_dict(self, proxy_universe_size=100):
        """method that creates a generator of proxies"""
        proxy_list: list = await self.get_proxy_universe(
            proxy_universe_size=proxy_universe_size,
            return_as_list_of_dicts=True,
            shuffle_results=True,
        )
        if proxy_list is None:
            return
        for proxy in proxy_list:
            yield proxy


class ProxyHandler(MySQLProxy):
    """Class proxy"""

    def __init__(self, proxy_universe_size: int = 100, async_loop=None):
        super().__init__(proxy_universe_size=proxy_universe_size)
        # Proxy generator
        self.proxy_yield_lock = asyncio.Lock()
        self.next_proxy_yield_lock = asyncio.Lock()
        self.print_proxy_lock = asyncio.Lock()
        self.proxy_generator = None

        # Initiate variables
        if async_loop is None:
            async_loop = asyncio.get_event_loop()
        async_loop.run_until_complete(
            self._set_proxy_generator(proxy_universe_size=self.proxy_universe_size))

    async def _set_proxy_generator(self, proxy_universe_size: Optional[int] = 100):
        async with self.proxy_yield_lock:
            self.proxy_yield = self.refresh_generator_proxy_dict(
                proxy_universe_size=proxy_universe_size
            )

    async def _print(self, str_to_print: object, verbose: bool = False):
        async with self.print_proxy_lock:
            if verbose:
                str_to_print = str(str_to_print)
                print(str_to_print)
            else:
                print(".", end="")

    @staticmethod
    def get_requests_proxies_as_dict(full_url: str) -> dict:
        """return dict {http:full_proxy_url, https:full_proxy_url using a full_proxy_url}"""
        return {"http": full_url, "https": full_url}

    async def get_next_proxy_from_generator(self):
        """Return a generator of proxy dicts which include country, score and else.
        At stop iteration, the generator is refreshed"""
        async with self.next_proxy_yield_lock:
            while True:
                try:
                    return await self.proxy_yield.__anext__()
                except StopAsyncIteration as ex:
                    logger.debug(f"Handled error: {ex}")
                    await self._set_proxy_generator()


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    my_getter = MySQLProxy()
    proxy_sql_query = """
    SELECT * FROM tbl_proxy_url ORDER BY error_count DESC LIMIT 10 
    """
    loop = asyncio.get_event_loop()

    print(loop.run_until_complete(my_getter.fetch_all_as_df(sql_query=proxy_sql_query)))
    my_proxy = ProxyHandler()
    counter: int = 1
    while counter < 10_000:
        proxy_dict = loop.run_until_complete(my_proxy.get_next_proxy_from_generator())
        print(proxy_dict["full_url"], proxy_dict["proxy_country"])
        counter += 1
