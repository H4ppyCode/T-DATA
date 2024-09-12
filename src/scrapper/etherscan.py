from typing import Optional, List 
from dataclasses import dataclass
import bs4
from scrapper.utils import bs_find_all


@dataclass
class EthTransaction:
    txid: str = ""
    method: str = ""
    blockno: int = 0
    amount: float = 0
    value: float = 0
    fee: float = 0
    age: str = ""
    from_id: str = ""
    from_name: Optional[str] = ""
    to_id: str = ""
    to_name: Optional[str] = ""

def get_transactions() -> List[EthTransaction]:
    result: List[EthTransaction] = []
    for tx_row in bs_find_all("https://etherscan.io/txs", "tr", user_agent="Mozilla/5.0")[1:]:
        tx_row: bs4.element.Tag
        columns: List[bs4.element.Tag] = tx_row.find_all("td")
        tx = EthTransaction()
        tx.txid = columns[1].find('a', {"class": "myFnExpandBox_searchVal"}).text
        tx.method = columns[2].text
        tx.blockno = int(columns[3].text)
        tx.age = columns[4].text
        tx.from_id = columns[7].find("a", {'title': 'Copy Address'}).attrs['data-clipboard-text']
        tx.from_name = columns[7].find("a").text.strip()
        if not tx.from_name or tx.from_name.startswith("0x") or tx.from_name == tx.from_id:
            tx.from_name = None
        tx.to_id = columns[9].find("a", {'title': 'Copy Address'}).attrs['data-clipboard-text']
        tx.to_name = columns[9].find("a").text.strip()
        if tx.to_name.startswith("0x"):
            tx.to_name = None
        values = columns[10].find_all("span")
        tx.amount = float(values[0].text[:-4])
        tx.value = float(values[1].text[1:].replace(",", ""))
        tx.fee = float(columns[11].text)
        result.append(tx)
    return result

