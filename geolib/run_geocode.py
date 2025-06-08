from geocode import geocode_helper
import click
import os

@click.command()
@click.option('--base-path', default=r'c:\Users\dxwil\git\voting_data\data', help='should be state codes under this directory')
def walk_data_dir(base_path):
    for state in os.listdir(base_path):
        for county in os.listdir(os.path.join(base_path, state)):
            print(state, county)
            input_file  = os.path.join(base_path, state, county, 'addresses.csv')
            output_file = os.path.join(base_path, state, county, f'{county}_{state}_addresses_out.csv')
            if os.path.exists(input_file):
                geocode_helper(str(input_file), str(output_file), state)


if __name__ == '__main__':
    walk_data_dir()

