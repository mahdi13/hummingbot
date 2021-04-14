from typing import Dict, Any


class FarhadmarketAuth():
    """
    Auth class required by FarhadMarket API
    """

    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key
        self.secret_key = secret_key

    def get_headers(self) -> Dict[str, Any]:
        """
        Generates authentication headers required by farhadmarket
        :return: a dictionary of auth headers
        """
        return {
            "X-API-KEY": self.api_key,
            "X-API-SECRET": self.secret_key,
            "Content-Type": "application/x-www-form-urlencoded",
        }
