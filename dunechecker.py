# from utils import get_main_wallet, get_all_wallets
from requests import Session
from pyuseragents import random as random_useragent
import json
from loguru import logger
from web3 import Web3
from multiprocessing.dummy import Pool
from enum import Enum
import csv
import math

import os

class queryID(Enum):
    checkUSD        = 2350073
    checkETH        = 2350748
    checkBNBChain   = 2344893


threads = 1
pool    = Pool(threads)

query_res = {}
for q in queryID:
    query_res[q.name] = []

def round_to(num, digits=2):
    try:
        if num == 0: return 0
        scale = int(-math.floor(math.log10(abs(num - int(num))))) + digits - 1
        if scale < digits: scale = digits
        return round(num, scale)
    except: return num

def search_wallet_in_table(d):
    _data = d["q"]
    name_column = d["column"]
    wallet = d["wal"]

    name_query = d["name"]
    for row in _data:
        if row[name_column] == wallet.lower():
            query_res[name_query].append({"address": wallet, "user_tx_count": row['user_tx_count'], "user_tx_value": row['usd_tx_value']})

def check_wallets(wallets):
    for enum_q in queryID:
        if os.path.exists(f"{enum_q.name}.txt"):
            os.remove(f"{enum_q.name}.txt")


    session = Session()
    session.headers.update({
                'user-agent': random_useragent(),
                'connection': 'keep-alive',
                'origin': 'https://dune.com',
                'referer': 'https://dune.com/',
                'content-type': 'application/json',
    })

    url = 'https://core-hsr.dune.com/v1/graphql'
    url_api = 'https://app-api.dune.com/v1/graphql'

    q_resp_arr = []

    for q in queryID:
        query_template = {
            "operationName": "GetResult",
            "variables": {
                "query_id": q.value,
                "parameters": []
            },
            "query": "query GetResult($query_id: Int!, $parameters: [Parameter!]!) {\n  get_result_v3(query_id: $query_id, parameters: $parameters) {\n    job_id\n    result_id\n    error_id\n    __typename\n  }\n}\n"
        }

        data_queryId = session.post(url, json=query_template)
        q_resp = json.loads(data_queryId.content)["data"]["get_result_v3"]

        query_template2 = {
            "operationName": "GetExecution",
            "variables": {
                "execution_id": q_resp["result_id"],
                "query_id": q.value,
                "parameters": []
            },
            "query": "query GetExecution($execution_id: String!, $query_id: Int!, $parameters: [Parameter!]!) {\n  get_execution(\n    execution_id: $execution_id\n    query_id: $query_id\n    parameters: $parameters\n  ) {\n    execution_queued {\n      execution_id\n      execution_user_id\n      position\n      execution_type\n      created_at\n      __typename\n    }\n    execution_running {\n      execution_id\n      execution_user_id\n      execution_type\n      started_at\n      created_at\n      __typename\n    }\n    execution_succeeded {\n      execution_id\n      runtime_seconds\n      generated_at\n      columns\n      data\n      __typename\n    }\n    execution_failed {\n      execution_id\n      type\n      message\n      metadata {\n        line\n        column\n        hint\n        __typename\n      }\n      runtime_seconds\n      generated_at\n      __typename\n    }\n    __typename\n  }\n}\n"
        }

        data = session.post(url_api, json=query_template2)
        q_resp_data = json.loads(data.content)["data"]["get_execution"]["execution_succeeded"]["data"]
        q_resp_arr.append({q.name: q_resp_data})


    logger.info('arr q_checkUSD_map')
    q_checkUSD_map = [{"q": q_resp_arr[list(queryID).index(queryID.checkUSD)][queryID.checkUSD.name], "column": "sender", "wal": wal, "name": queryID.checkUSD.name} for wal in wallets]
    logger.info('arr q_checkEth_map')
    q_checkEth_map = [{"q": q_resp_arr[list(queryID).index(queryID.checkETH)][queryID.checkETH.name], "column": "sender", "wal": wal, "name": queryID.checkETH.name} for wal in wallets]
    logger.info('arr q_checkBNBChain_map')
    q_checkBNBChain_map = [{"q": q_resp_arr[list(queryID).index(queryID.checkBNBChain)][queryID.checkBNBChain.name], "column": "sender", "wal": wal, "name": queryID.checkBNBChain.name} for wal in wallets]

    logger.info('start q_checkUSD_map')
    pool.map(search_wallet_in_table, q_checkUSD_map)
    q_checkUSD_map = []
    logger.info('start q_checkEth_map')
    pool.map(search_wallet_in_table, q_checkEth_map)
    q_checkEth_map = []
    logger.info('start q_checkBNBChain_map')
    pool.map(search_wallet_in_table, q_checkBNBChain_map)
    q_checkBNBChain_map = []

def create_file(file_name, query_res):

    with open(f'{outfile}{file_name}.csv', 'w', newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)

        spamwriter.writerow([
            'number', 
            'wallet', 
            'checkUSD-count', 
            'checkUSD-value', 
            'checkETH-count', 
            'checkETH-value', 
            'checkBNBChain-count', 
            'checkBNBChain-value'
            ])

        wallets = {}

        for data in query_res.items():
            for items in data[1]:
                address = items['address']
                wallets.update({address : {}})

        for data in query_res.items():

            type_check = data[0]

            for items in data[1]:
                address = items['address']
                user_tx_count = items['user_tx_count']
                user_tx_value = items['user_tx_value']
                wallets[address].update({type_check : {'user_tx_count' : user_tx_count, 'user_tx_value' : user_tx_value}})

        # create csv
        zero = 0
        for wallet in wallets.items():
            zero += 1

            massive = []

            massive.append(zero)
            massive.append(wallet[0])

            try:
                massive.append(wallet[1]['checkUSD']['user_tx_count'])
                massive.append(round_to(wallet[1]['checkUSD']['user_tx_value']))
            except: 
                massive.append(0)
                massive.append(0)

            try:
                massive.append(wallet[1]['checkETH']['user_tx_count'])
                massive.append(round_to(wallet[1]['checkETH']['user_tx_value']))
            except: 
                massive.append(0)
                massive.append(0)

            try:
                massive.append(wallet[1]['checkBNBChain']['user_tx_count'])
                massive.append(round_to(wallet[1]['checkBNBChain']['user_tx_value']))
            except: 
                massive.append(0)
                massive.append(0)

            spamwriter.writerow(massive)

if __name__ == '__main__':

    outfile = ''
    with open(f"{outfile}wallets.txt", "r") as f:
        wallets = [row.strip() for row in f]

    check_wallets(wallets)

    file_name = 'results'
    create_file(file_name, query_res)


