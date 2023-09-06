import logging
import requests
import re
from typing import Dict, List


class MonitorMemoryRemains:
    def __init__(self):
        self._proxy = None
        self._api_url: str = ''
        self._metrics_to_fetch: List = ['redis_memory_max_bytes', 'redis_memory_used_bytes']

        self._match_pattern = re.compile(r'redis-m\d*')

    def _calculate_total_memory(self, response: Dict) -> float:
        total_memory = 0
        for each in response['data']['result']:
            metric = each['metric']
            if re.match(self._match_pattern, metric['alias']):
                total_memory += float(each['value'][-1])
        return total_memory

    def _send_request(self, param: str) -> Dict:
        params = {'query': param}
        try:
            return requests.get(self._api_url, proxies=self.__proxy, params=params).json()
        except requests.HTTPError as e:
            print(f'Error while connecting to {self._api_url} with params {params}: {e}')

    def _get_responses(self) -> List[dict]:
        try:
            responses = []
            for metric in self._metrics_to_fetch:
                response = self._send_request(metric)
                responses.append(response)
            return responses
        except AttributeError as e:
            print(e)

    @staticmethod
    def _convert_bytes_to_gb(memory: float) -> str:
        return str(round(memory / 1024 / 1024 / 1024, 2)) + ' Gb'

    def calculate_memory_remains(self) -> str:
        responses = self._get_responses()
        memory_remains = abs(self._calculate_total_memory(responses[0]) - self._calculate_total_memory(responses[-1]))
        return self._convert_bytes_to_gb(memory_remains)


def throw_memory_warn_info() -> None:
    logging.basicConfig(format='%(levelname)s %(asctime)s:  %(message)s')
    warn_string = 'In Redis, {} is available for loading.'
    try:
        memory = MonitorMemoryRemains().calculate_memory_remains()
        logging.warning(warn_string.format(memory))
    except (KeyError, ValueError) as e:
        print(f'API response structure has been changed: {e}')
