import os
import argparse
import yaml
import requests
import src.infrastructure.logs as lcologs

def upload_lightcurve(params, log=None):

    decision_to_upload = decide_whether_to_upload(params, log=log)

    if decision_to_upload:
        # Load authentication information for the TOM system
        tom_config = yaml.safe_load(open(params['tom_config_file']))
        tom_config['login'] = (tom_config['tom_user_id'], tom_config['tom_password'])

        (target_pk, target_groups) = get_target_id(params, tom_config, log=log)

        if target_pk:
            existing_datafiles = list_dataproducts(params, tom_config, target_pk, log=log)

            delete_old_datafile_version(params, tom_config, existing_datafiles, log=log)

        upload_datafile(params, tom_config, target_pk, target_groups, log=log)

def decide_whether_to_upload(params, log=None):

    upload = True

    if not os.path.isfile(params['file_path']):
        upload = False
        lcologs.log(
            '-> Lightcurve upload to TOM aborted due to missing data file '+params['file_path'],
            'warning',
            log=log
        )

    file_lines = open(params['file_path'],'r').readlines()

    if len(file_lines) < 6:     # Allows for header line
        upload = False
        lcologs.log(
            '-> Lightcurve has too few datapoints ('+str(len(file_lines))+') to upload to TOM ',
            'warning',
            log=log
        )

    if upload:
        lcologs.log(
            '-> Lightcurve has passed sanity checks and will be uploaded to TOM',
            'warning',
            log=log
        )

    return upload

def concat_urls(base_url,extn_url):
    """Function to concatenate URL components without unnecessary duplication
    of /"""

    if base_url[-1:] == '/':
        base_url = base_url[:-1]
    if extn_url[0:1] == '/':
        extn_url = extn_url[1:]

    return base_url+'/'+extn_url

def get_target_id(params, tom_config, log=None):
    """
    Function queries the TOM to find the unique primary key identifier for the
    target name given.  This parameter is required for data upload.
    """

    targetid_url = concat_urls(tom_config['url'], 'api/targets')

    target_pk = None
    target_groups = []
    ur = {'name': params['target_name']}
    response = requests.get(targetid_url, auth=tom_config['login'], params=ur).json()

    if 'results' in response.keys() and len(response['results']) == 1:
        target_pk = response['results'][0]['id']
        for group in response['results'][0]['groups']:
            target_groups.append(group['id'])

        lcologs.log(
            'TOM identified target ' + params['target_name'] + ' as target ID=' + str(target_pk),
            'info',
            log=log
        )

    elif 'results' in response.keys() and len(response['results']) == 0:
        lcologs.log('Targetname ' + params['name'] + ' unknown to TOM', 'warning', log=log)

    elif 'results' in response.keys() and len(response['results']) > 1:
        lcologs.log(
            'Ambiguous targetname ' + params['name'] + ' multiple entries in TOM',
            'warning',
            log=log
        )

    else:
        lcologs.log('No response from TOM.  Check login details and URL?', 'warning', log=log)

    return target_pk, target_groups

def upload_datafile(params, tom_config, target_pk, target_groups, log=None):
    """Function uploads photometry data to the TOM"""

    ur = {'target': target_pk, 'data_product_type': 'photometry', 'groups': target_groups}

    file_data = {'file': (params['file_path'], open(params['file_path'],'rb'))}

    dataupload_url = concat_urls(tom_config['url'], 'api/dataproducts/')

    response = requests.post(dataupload_url, data=ur, files=file_data, auth=tom_config['login'])

    lcologs.log(
        'Uploaded lightcurve file to TOM at URL: ' + repr(response.url) \
        + ' with response: '+repr(response.text),
        'info',
        log=log
    )

def list_dataproducts(params, tom_config, target_pk, log=None):
    """Function to return a list of dataproducts for the given target that
    have already been uploaded to the TOM"""

    dataupload_url = concat_urls(tom_config['url'], 'api/dataproducts/')

    ur = {'data_product_type': 'photometry', 'limit': 99999}

    # List endpoint does not currently support queries specific to target ID
    #response = requests.get(dataupload_url, params=ur, auth=login).json()
    response = requests.get(dataupload_url, params=ur, auth=tom_config['login']).json()

    existing_datafiles = {}
    for entry in response['results']:
        if entry['target'] == target_pk:
            existing_datafiles[os.path.basename(entry['data'])] = entry['id']

    if len(existing_datafiles) > 0:
        lcologs.log(
            'Found existing datafiles for target ' + params['target_name'] +\
                ', ID=' + str(target_pk) + ' in the TOM:'
            + '\n'.join(existing_datafiles.keys()),
            'info',
            log=log
        )
    else:
        lcologs.log(
            'No existing datafiles in TOM for target ' + params['name'],
            'info',
            log=log
        )

    return existing_datafiles

def delete_old_datafile_version(params, tom_config, existing_datafiles,  log=None):
    """Function to find and delete any existing entry in the TOM for the
    datafile to be uploaded"""

    # Due to automatic suffixes added by the data ingest processor, the only
    # way to identify datasets from the same telescope is the first
    # section of the filename, which must be distinctively named.
    dataupload_url = concat_urls(tom_config['url'], 'api/dataproducts/')

    lcologs.log('Searching TOM system for previous similar datafiles', 'info', log=log)

    for fname, id in existing_datafiles.items():
        if params['data_label'] in fname:
            file_pk = id
            delete_data_url = concat_urls(dataupload_url, str(file_pk))
            response = requests.delete(delete_data_url, auth=tom_config['login'])
            lcologs.log(
                'Attempted to remove old datafile from TOM with response: ' + repr(response.text),
                'info',
                log=log
            )


def get_args():

    parser = argparse.ArgumentParser()
    parser.add_argument('red_dir', help='Path to the reduction directory')
    parser.add_argument('file_path', help='Path to the data file')
    parser.add_argument('data_label', help='Dataset label in the TOM')
    parser.add_argument('target_name', help='Name of target in TOM system')
    parser.add_argument('tom_config_file', help='Path to TOM configuration file')
    args = parser.parse_args()

    params = {
        'file_path': args.file_path,
        'red_dir': args.red_dir,
        'data_label': args.data_label,
        'target_name': args.target_name,
        'tom_config_file': args.tom_config_file
    }

    return params

if __name__ == '__main__':

    params = get_args()

    log = lcologs.start_log(params['red_dir'], 'tom_upload')

    upload_lightcurve(params, log=log)

    lcologs.close_log(log)
