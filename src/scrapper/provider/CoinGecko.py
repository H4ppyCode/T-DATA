from typing import List, Dict
from pycoingecko import CoinGeckoAPI


import time


class CoinGecko:
    def __init__(self):
        self.api = CoinGeckoAPI(demo_api_key='CG-q2DSLucbpJh1nyrBQjXWPdXM')

    def get_prices(self, coins: List[str] = ['bitcoin', 'ethereum'], currencies: List[str] = ['usd']) -> Dict[str, Dict[str, int]]:
        """Return the prices of the requested coins in the requested currency.
        
        Example result: {'bitcoin': {'usd': 100898}, 'ethereum': {'usd': 3924.34}}
        """
        ids = ','.join(coins)
        dest_currencies = ','.join(currencies)
        prices = self.api.get_price(ids, dest_currencies)
        time.sleep(5)
        return prices

    def get_price_24h_ago(self, coin: str = 'bitcoin', currency: str = 'usd') -> int:
        """Return the price of the requested coin 24 hours ago in the requested currency."""
        now = int(time.time())
        twenty_four_hours_ago = now - 86400
        r = self.api.get_coin_market_chart_range_by_id(coin, currency, twenty_four_hours_ago,                     twenty_four_hours_ago + 3600)
        time.sleep(1)
        return r['prices'][0][1]


if __name__ == "__main__":
    a = CoinGecko()
    while True:
        print(a.get_prices())
        print(a.get_price_24h_ago())
