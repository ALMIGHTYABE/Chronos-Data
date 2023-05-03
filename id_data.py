import requests
import pandas as pd
import yaml
from application_logging.logger import logger
from web3 import Web3
from web3.middleware import validation


# Params
params_path = "params.yaml"


def read_params(config_path):
    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config


config = read_params(params_path)

try:
    logger.info("ID Data Started")

    # Params Data
    provider_url = config["web3"]["provider_url"]
    pair_factory = config["web3"]["pair_factory"]
    pair_factory_abi = config["web3"]["pair_factory_abi"]
    amm_abi = config["web3"]["amm_abi"]
    ve_contract = config["web3"]["ve_contract"]
    voter_abi = config["web3"]["voter_abi"]

    # Web3
    validation.METHODS_TO_VALIDATE = []
    w3 = Web3(Web3.HTTPProvider(provider_url, request_kwargs={"timeout": 60})) 
    contract_instance = w3.eth.contract(address=pair_factory, abi=pair_factory_abi)
    max_pairs = contract_instance.functions.allPairsLength().call()
    
    pair_list = []
    for i in range(max_pairs):
        pair_list.append(contract_instance.functions.allPairs(i).call())

    ids_df = pd.DataFrame(pair_list)
    ids_df.columns = ['id']

    names = []
    for address in ids_df["id"]:
        address = w3.toChecksumAddress(address)
        contract_instance = w3.eth.contract(address=address, abi=amm_abi)
        names.append({"name": contract_instance.functions.symbol().call(), "address": address})

    ids_df = pd.DataFrame(names)
    ids_df[["type", "pair"]] = ids_df["name"].str.split("-", 1, expand=True)
    ids_df.drop(["pair"], axis=1, inplace=True)


    # Solidly Pools
    contract_instance = w3.eth.contract(address=ve_contract, abi=voter_abi)
    gauges = []
    bribe_ca = []
    fee_ca = []
    for address in ids_df["address"]:
        address = w3.toChecksumAddress(address)
        gauge = contract_instance.functions.gauges(address).call()
        gauges.append(gauge)
        bribe_ca.append(contract_instance.functions.external_bribes(gauge).call())
        fee_ca.append(contract_instance.functions.internal_bribes(gauge).call())
    ids_df["gauges"] = gauges
    ids_df["bribe_ca"] = bribe_ca
    ids_df["fee_ca"] = fee_ca

    ids_df.to_csv("data/ids_data.csv", index=False)

    logger.info("ID Data Ended")
except Exception as e:
    logger.error("Error occurred during ID Data process. Error: %s" % e)