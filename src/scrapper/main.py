import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import scrapper.btcscan
import scrapper.etherscan
from scrapper.mempool import MempoolWs

if __name__ == "__main__":
    print(scrapper.btcscan.get_transactions())
    print(scrapper.etherscan.get_transactions())
    MempoolWs()
