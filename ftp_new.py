from multiprocessing import Pool
from functools import partial
from ftplib import FTP
from pathlib import Path
import json


class FileUploader:
    def __init__(self, path_from, path_to):
        self.path_from = path_from  # откуда копировать файл
        self.path_to = path_to  # куда копировать файл

    @staticmethod
    def get_ftp_connection(host, port, name, passwd):  # метод для конекта к ftp серверу
        ftp = FTP('')
        ftp.connect(host, port)
        ftp.login(name, passwd)
        return ftp

    def upload(self, ftp_data):  # метод непосредственно загрузки файла
        with open(self.path_from, 'rb') as f:
            try:
                ftp = self.get_ftp_connection(ftp_data['host'], ftp_data['port'], ftp_data['user'], ftp_data['pass']) #запуск метода для подключения к серверу
            except Exception as msg:  # в случае неудачного подключения выдаст сообщение об ошибке
                print("Error {}".format(msg))
                exit(1)
            p = Path(self.path_from)
            try:
                ftp.cwd(self.path_to)  # переход к директории куда скопировать файл
            except Exception:  # в случае ошибки создание этой директории и переход к ней
                ftp.mkd(self.path_to)
                ftp.cwd(self.path_to)
            ftp.storbinary('STOR %s' % p.name, f)  # загрузка файла на ftp сервер


class PoolFtp:
    def __init__(self, config_name):
        self.config_data = self._get_config(config_name)

    @staticmethod
    def _get_config(config_name):  #метод для получения данных конфигурации из json
        with open(config_name, 'r') as f:
            return json.load(f)

    def copy_file(self, files):  # метод, который передает информацию о файле в функцию загрузки на сервер
        FileUploader(files[0], files[1]).upload(self.config_data['ftp'])

    def run(self):
        pool = Pool(self.config_data['pool_num'])  # многопоточность
        pool.map(self.copy_file, self.config_data['files'])


def main():
    copy = PoolFtp('./config.json')
    copy.run()  # запуск основного метода


if __name__ == "__main__":
    main()
