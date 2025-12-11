import logging
from collections.abc import Generator

import requests

logger = logging.getLogger(__name__)


class RestAdapter:
    """Adapter for fetching data from REST API."""

    BASE_URL = 'https://jaffle-shop.scalevector.ai/api/v1/'

    def _generate_endpoint(self, endpoint: str) -> str:
        return self.BASE_URL + endpoint

    def get_data(self, endpoint: str) -> Generator[list[dict[str, str]]]:
        """Fetch data from API endpoint and yield batches."""
        full_url = self._generate_endpoint(endpoint)
        logger.info('Fetching data from: %s', full_url)

        try:
            response = requests.get(url=full_url, timeout=30)
            response.raise_for_status()
            data_out = response.json()

            if isinstance(data_out, list):
                yield data_out
            else:
                logger.warning('API returned non-list data, wrapping in list')
                yield [data_out]

        except requests.RequestException as e:
            logger.error('Failed to fetch data from %s: %s', full_url, e)
            raise
