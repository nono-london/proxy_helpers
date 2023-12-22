from time import sleep

import requests


class ProxyChecker:
    def __init__(self, timeout: int = 25):
        self.base_url = "https://httpbin.org/ip"
        self.timeout: int = timeout

    def check_proxy(self, proxy_full_url: str, max_attempts: int = 10) -> bool:
        """Return True if IP returned by request if the same as proxy_full_url's url"""
        # https://www.scrapingbee.com/blog/python-requests-proxy/#:~:text=To%20use%20a%20proxy%20in,webpage%20you're%20scraping%20from.
        proxies = {
            "http": "http://" + proxy_full_url,
            "https": "http://" + proxy_full_url,
        }
        while max_attempts > 0:
            print(30 * "#")
            print(f"Attempts left: {max_attempts}")
            try:
                response = requests.get(
                    url=self.base_url, timeout=self.timeout, proxies=proxies
                )
                print(f"Proxy request status code: {response.status_code}")
                print(f'Proxy IP: {proxy_full_url.split(":")[0]}')
                print(f'Response IP: {response.json()["origin"]}')

                if response.status_code == 200:
                    return True
            except Exception as ex:
                print(
                    f"Error with proxy: {proxy_full_url}\n"
                    f"Error is: {ex.__class__.__name__}"
                )
                max_attempts -= 1
                sleep(5)
                continue

            max_attempts -= 1
            sleep(5)

        return False


if __name__ == "__main__":
    proxy_checker = ProxyChecker()
    print(proxy_checker.check_proxy(proxy_full_url="167.235.63.238:3128"))
