# main.py
import click
import json
import yaml

from pipeline.flow import Builder

def process(config):
    bldr = Builder(config)
    df = bldr.run()
    return df

''' Main file loads configuration '''
@click.command()
@click.argument('file_path', type=click.Path(exists=True))
def start(file_path):
    with open(file_path) as data_file:
        if file_path.endswith("json"):    
            config = json.load(data_file) 
        else:
            config = yaml.load(data_file)   
        return process(config)

if __name__ == '__main__':
    start()
