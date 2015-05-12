#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

import sys
import time
import argparse

from os import getenv
from os.path import expanduser
from threading import Thread

try:
    from Queue import Queue
except:
    from queue import Queue

import dropbox


PY2 = (sys.version_info[0] == 2)
if PY2:
    input = raw_input

API_KEY = getenv('DUHPY_API_KEY', 'YOUR_API_KEY')
APP_SECRET = getenv('DUHPY_APP_SECRET', 'YOUR_APP_SECRET')

CONFIG_PATH = expanduser('~/.duhpy')
RED = '\033[91m'
NO = '\033[0m'


class APICrawler(object):
    def __init__(self, client, nb_threads=10):
        self.client = client
        self.values = Queue()
        self.q = Queue()

        for i in range(nb_threads):
            worker = Thread(target=self.worker)
            worker.daemon = True
            worker.start()

    def run(self, path='/'):
        self.q.put(path)
        self.q.join()

        total_size = 0
        self.values.put('--END--')
        for i in iter(self.values.get, '--END--'):
            total_size += i
        return total_size

    def worker(self):
        while True:
            path = self.q.get()
            #print(path)
            try:
                json = self.client.metadata(path)
                if not is_dir(json):
                    self.values.put(json['bytes'])

                dir_size = 0
                for item in json['contents']:
                    if is_dir(item):
                        self.q.put(item['path'])
                    else:
                        dir_size += item['bytes']
                self.values.put(dir_size)
            except dropbox.rest.ErrorResponse as e:
                if e.status == 429:
                    #print(RED, '*** Dropbox API rate limit reached ***', NO)
                    time.sleep(1.5)
                    self.q.put(path)

            self.q.task_done()


def request_token():
    if API_KEY == 'YOUR_API_KEY' or APP_SECRET == 'YOUR_APP_SECRET':
        print('Please, see the documentation https://github.com/dethi/duhpy')
        sys.exit(1)

    flow = dropbox.client.DropboxOAuth2FlowNoRedirect(API_KEY, APP_SECRET)
    authorize_url = flow.start()
    print('1. Go to: ', authorize_url)
    print('2. Click "Allow".')
    print('3. Copy the authorization code.')
    code = input('Enter the authorization code here: ').strip()
    try:
        access_token, user_id = flow.finish(code)
    except:
        print('[ERROR] Invalid code')
        access_token = None
    return access_token


def is_dir(metadata):
    if metadata is None:
        return False
    return metadata["is_dir"]


def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return '{:.1f} {}{}'.format(num, unit, suffix)
        num /= 1024.0
    return '{:.1f} {}{}'.format(num, 'Yi', suffix)


def main():
    parser = argparse.ArgumentParser(
        prog='duhpy',
        description='`du -h` command for Dropbox (online).')
    parser.add_argument('path', metavar='PATH', type=str, nargs='+')
    parser.add_argument('--version', action='version', version='%(prog)s 1.0')
    args = parser.parse_args()

    try:
        with open(CONFIG_PATH, 'r') as f:
            token = f.read()
    except IOError:
        token = None
        while (token is None):
            token = request_token()
        with open(CONFIG_PATH, 'w') as f:
            f.write(token)

    client = dropbox.client.DropboxClient(token)

    crawler = APICrawler(client)
    path_len = min(max(max(map(len, args.path)), 13), 64)

    print('{0:^{2}} | {1:^13}'.format('PATH', 'SIZE', path_len))
    print('{0:-<{1}}+{0:-<14}'.format('-', path_len + 1))
    for path in args.path:
        result = crawler.run(path)
        print('{0:<{2}.{2}} | {1:>13}'.format(path, sizeof_fmt(result),
                                              path_len))
    print()


if __name__ == '__main__':
    main()
