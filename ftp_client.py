from multiprocessing import Pool
from functools import partial
from ftplib import FTP
from pathlib import Path
import json


def get_config(config_name):
    with open(config_name, 'r') as f:
        return json.load(f)


def get_ftp_connection(host, port, name, passwd):
    ftp = FTP('')
    ftp.connect(host, port)
    ftp.login(name, passwd)
    return ftp


def upload(file_name, ftp_data):
    with open(file_name, 'rb') as f:
        with get_ftp_connection(ftp_data['host'], ftp_data['port'], ftp_data['user'], ftp_data['pass']) as ftp:
            p = Path(file_name)
            ftp.storbinary('STOR %s' % p.name, f)


def main():
    config_data = get_config('./config.json')

    pool = Pool(config_data['pool_num'])
    pool.map(partial(upload, ftp_data=config_data['ftp']), config_data['files'])


if __name__ == "__main__":
    main()