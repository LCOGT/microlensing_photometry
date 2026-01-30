import os
import argparse
import yaml
from microlensing_photometry.infrastructure import logs as lcologs

def create_red_config(dmconfig, red_params, red_dir, log=None):
    """
    Function to create the reduction_configuration YAML file and populate it
    with the parameters of a specific dataset.

    Parameters
    ----------
    dmconfig  dict  Configuration of the Data Management process handling the reduction configuration
                of the form:  {'default_red_config': /path/to/default/conf/file.yaml }
    red_params dict Parameter values in the default reduction YAML file to be configured
                Note that the parameter names need to match those in the reduction configuration, i.e.
                {'name':<name>, 'RA': <sexigesmial string>, 'Dec': <sexigesimal string>}
    red_dir     string Path to the target's reduction directory
    log  log instance   [optional] Logger object
    """

    # Load default pipeline config
    if 'default_red_config' not in dmconfig.keys():
        err = 'No default reduction configuration parameter set in data management configuration'
        lcologs.log(err, 'error', log=log)
        raise IOError(err)
    elif not os.path.isfile(dmconfig['default_red_config']):
        err = 'Cannot find the default reduction configuration file ' + dmconfig['default_red_config']
        lcologs.log(err, 'error', log=log)
        raise IOError(err)
    red_config = yaml.safe_load(open(dmconfig['default_red_config']))

    # Populate the template configuration with the parameters specific to this dataset
    for key, value in red_params.items():
        red_config['target'][key] = value

    # Output the customized configuration to the appropriate reduction directory
    file_path = os.path.join(red_dir, 'reduction_configuration.yaml')
    with open(file_path, 'w') as f:
        yaml.dump(red_config, f, sort_keys=False, default_flow_style=False)

    lcologs.log(
        'Created reduction configuration for ' + red_dir,
        'info', log=log
    )

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('default_red_config', help='Path to default reduction configuration YAML file')
    parser.add_argument('target', help='Name of target')
    parser.add_argument('ra', help='RA of target as sexigesimal string(use ra= syntax)')
    parser.add_argument('dec', help='Dec of target as sexigesimal string (use dec= syntax)')
    parser.add_argument('red_dir', help='Path to reduction directory')
    args = parser.parse_args()

    dmconfig = {'default_red_config': args.default_red_config}
    red_params = {'name': args.target, 'RA': args.ra.split('=')[-1], 'Dec': args.dec.split('=')[-1]}

    return dmconfig, red_params, args.red_dir


if __name__ == '__main__':
    dmconfig, red_params, red_dir = get_args()
    create_red_config(dmconfig, red_params, red_dir)