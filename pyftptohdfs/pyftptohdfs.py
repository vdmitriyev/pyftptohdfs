#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Viktor Dmitriyev'
__license__ = 'MIT'
__version__ = '1.0.0'
__maintainer__ = '-'
__email__ = ''
__date__ = '05.04.2016'
__description__ = 'Transporting files from FTP to HDFS'

import os
import sys
import uuid
import argparse
import platform
import subprocess

from ftplib import FTP

# default settings
default_ftp_ip = 'speedtest.tele2.net'
default_ftp_user = 'anonymous'
default_ftp_password = 'anonymous'
default_ftp_target_folder = '/'
default_hdfs_target_path = '/tmp/'
default_hdfs_target_folder = 'pyftptohdfs'
# default_hfds_host = 'localhost'
# default_hfds_port = 8020

# settings
file_size_limit_mb = 2
file_size_limit_b = file_size_limit_mb * 1014 * 1024

def launch_bash_command(params, msg=None, info_only=False):
    """ Launching bash command """

    if msg is not None:
        print ('{0}'.format(msg))

    cmd = ' '.join(params)
    print ('[i] following command will be executed:\n{0}'.format(cmd))

    if not info_only:
        subprocess.call(cmd, shell=True, cwd='.')

def gettext(ftp, filename, outfile=None):
    """ fetch a text file from FTP """
    if outfile is None:
        outfile = sys.stdout
    # use a lambda to add newlines to the lines read from the server
    ftp.retrlines("RETR " + filename, lambda s, w=outfile.write: w(s+"\n"))

def getbinary(ftp, filename, outfile=None):
    """ fetch a binary file from FTP """
    if outfile is None:
        outfile = sys.stdout
    ftp.retrbinary("RETR " + filename, outfile.write)

def copy_from_ftp(params):
    """ Copy files from FTP """

    ftp = FTP(params['ftp_ip'])
    ftp.login(params['ftp_user'], params['ftp_password'])

    ftp_target_folder = params['ftp_target_folder']
    # target_local_file = params['target_local_file']

    ftp.cwd(ftp_target_folder)
    ftp_files_list = []
    ftp.dir(ftp_files_list.append)
    #print ('\n'.join(ftp_files_list))

    # verifying target directory on local machine
    local_target = 'dwn' + str(uuid.uuid4())[:4]
    if not os.path.exists(local_target):
        os.makedirs(local_target)

    for ftp_response in ftp_files_list:
        splitted_response = ftp_response.split()
        file_name = splitted_response[8]
        file_size = int(splitted_response[4])
        file_type = int(splitted_response[1]) # no documentation available on response of FTP LIST,
                                              # I hope that 1 equals to "real" files, not folders
        if file_size <= file_size_limit_b and file_type == 1:
            print ('[i] processing file from FTP {0}'.format(file_name))
            target_local_file = os.path.join(local_target, file_name)
            getbinary(ftp, file_name, open(target_local_file, 'wb'))
        else:
            print ('[i] file was ignored because of size or type, name {0}'.format(file_name))

    ftp.quit()

    return local_target

def copy_to_hdfs(params):
    """ Copy to HDFS"""

    # from hdfs3 import HDFileSystem
    # hdfs = HDFileSystem(host=hfds_host, port=hfds_port)


    # from snakebite.client import Client
    # from hdfs import InsecureClient
    # hdfs_client = InsecureClient('hdfs://{0}:{1}'.format(hfds_host, hfds_port), user='dmitriyev')
    # hdfs_fnames = hdfs_client.list('tmp')
    # print ('\n'.join(hdfs_fnames))

    # hdfs_client = Client(hfds_host, hfds_port, use_trash=False)

    # print ('[i] paths in tmp folder, just for debugging')
    # for x in hdfs_client.ls(['/tmp/']):
    #     print (x['path'])

    current_dir = os.path.dirname(os.path.abspath(__file__))
    local_path = os.path.join(current_dir, params['local_target'])
    hdfs_path = os.path.join(params['hdfs_target_path'], params['hdfs_target_folder'])
    hdfs_path_dest = os.path.join(hdfs_path, str(uuid.uuid1())[:4])

    launch_bash_command(params = ['hdfs', 'dfs', '-mkdir', hdfs_path],
                        msg = '[i] trying to create folder in HDFS {0}'.format(hdfs_path))

    launch_bash_command(params = ['hdfs', 'dfs', '-mkdir', hdfs_path_dest],
                        msg = '[i] trying to create folder in HDFS {0}'.format(hdfs_path_dest))

    launch_bash_command(params = ['hdfs', 'dfs', '-put', local_path, hdfs_path_dest],
                        msg = '[i] copying files from from local to remote HDFS\n \t local: {0}\n \t HDFS: {1}'.format(local_path, hdfs_path_dest))

def merge_files(params):
    """ Merging downloaded files"""

    bash_content = """#!/bin/bash

        INPUT={input}
        FILES_DELIMETER="<-----filescontentdelimeter----->"
        MERGED=merged_files_generated
        EXTENSION=*

        echo $MERGED

        if [ -f "$MERGED" ]
        then
            rm $MERGED
        fi

        for i in $INPUT/*.$EXTENSION; do echo  "$FILES_DELIMETER"; echo  "<-----$i----->"; cat "$i"; done > $INPUT/$MERGED"""

    target_dir = params['local_target']
    bash_file_name = 'files_merger_generated.sh'
    bash_content = bash_content.format(input=target_dir)

    # creating bash file for merging
    with open(bash_file_name, 'w') as the_file:
        the_file.write(bash_content)

    if platform.system() == 'Linux':
        launch_bash_command(['chmod', '+x', '{file}'.format(file=bash_file_name)], msg = "Creating bash file with merger.")
        launch_bash_command(['./' + bash_file_name, target_dir + '/'], msg = "Launching bash file with merger.")

    if platform.system() == 'Windows':
        launch_bash_command(['sh ' + bash_file_name, target_dir + '/'], msg = "Launching bash file with merger.")

def main(params):
    """ Main method """

    local_target = copy_from_ftp(params)
    params["local_target"] = local_target

    if not params['downloadonly']:
        merge_files(params)
        copy_to_hdfs(params)

if __name__ == '__main__':

    try:
        from logger import Logger
        sys.stdout = Logger()
    except Exception as ex:
        print ('[e] could not import custom logger module')

    # fetching input parameters
    parser = argparse.ArgumentParser(description=__description__)
    #args = {}

    # ftp_ip
    parser.add_argument(
        '--ftp_ip',
        help='specifies FTP address, default: {0}'.format(default_ftp_ip))
    parser.set_defaults(ftp_ip=default_ftp_ip)

    # ftp_user
    parser.add_argument(
        '--ftp_user',
        help='specifies FTP user, default: {0}'.format(default_ftp_user))
    parser.set_defaults(ftp_user=default_ftp_user)

    # ftp_password
    parser.add_argument(
        '--ftp_password',
        help='specifies FTP password, default: {0}'.format(default_ftp_password))
    parser.set_defaults(ftp_password=default_ftp_password)

    # ftp_target_folder
    parser.add_argument(
        '--ftp_target_folder',
        help='setting FTP target folder, default: {0}'.format(default_ftp_target_folder))
    parser.set_defaults(ftp_target_folder=default_ftp_target_folder)

    # downloadonly
    parser.add_argument(
        '--downloadonly',
        action='store_true',
        help='downloads only from FTP , default: {0}'.format(False))
    parser.set_defaults(downloadonly=False)

    # hdfs_target_path
    parser.add_argument(
        '--hdfs_target_path',
        help='setting HDFS target path, default: {0}'.format(default_hdfs_target_path))
    parser.set_defaults(hdfs_target_path=default_hdfs_target_path)

    # hdfs_target_folder
    parser.add_argument(
        '--hdfs_target_folder',
        help='setting HDFS target folder, default: {0}'.format(default_hdfs_target_folder))
    parser.set_defaults(hdfs_target_folder=default_hdfs_target_folder)

    args = parser.parse_args()
    args_dict = vars(args)
    #print(args_dict)

    main(args_dict)
