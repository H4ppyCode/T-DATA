from typing import Optional, List 
from dataclasses import dataclass, field
import requests
import dacite


@dataclass
class PrevOut:
    value: int
    scriptpubkey: str
    scriptpubkey_asm: str
    scriptpubkey_type: str
    scriptpubkey_address: Optional[str]

@dataclass
class VolumeIn:
    txid: str
    vout: int
    prevout: PrevOut
    scriptsig: str
    scriptsig_asm: str
    is_coinbase: bool
    sequence: int
    witness: List[str] = field(default_factory=list)

@dataclass
class Status:
    confirmed: bool
    block_height: Optional[int]
    block_time: Optional[int]
    block_hash: Optional[str]

@dataclass
class Transaction:
    txid: str
    version: int
    locktime: int
    size: int
    weight: int
    fee: int
    vin: List[VolumeIn]
    vout: List[PrevOut]
    status: Status


@dataclass
class TransactionShort:
    txid: str
    fee: int
    vsize: int
    value: int


TransactionList = List[Transaction]

    
def get_transactions() -> TransactionList:
    res = requests.get("https://btcscan.org/api/mempool/recent", headers={"accept":"application/json"})
    res.raise_for_status()
    txs: List[TransactionShort] = [dacite.from_dict(TransactionShort, tx) for tx in res.json()]
    result: TransactionList = []


    for tx in txs:
        details = requests.get(f"https://btcscan.org/api/tx/{tx.txid}", headers={"accept":"application/json"})
        details.raise_for_status()
        result.append(dacite.from_dict(Transaction, details.json()))
    return result
