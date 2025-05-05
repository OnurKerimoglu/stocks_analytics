import yaml
import streamlit as st

def load_configs(
        config_paths,
        env):
    print(f'loading configs for {env}')
    config = {}
    # load dwh config
    dwh_fpath = config_paths['dwh']
    with open(dwh_fpath, 'r') as f:
        config_dwh = yaml.safe_load(f)
    dwh_shared = config_dwh['shared']
    dwh_env = config_dwh[env]
    config['DWH'] = {**dwh_shared, **dwh_env} 
    return config


if __name__ == '__main__':
    config = load_configs('dev')
    print(config)