#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import sys
import re
import copy
import time
import subprocess
import requests
import tarfile
from subprocess import PIPE

from argparse import ArgumentParser
import pymysql.cursors

TEST_DB_NAME = 'ssb_sf02'
TEST_TABLE_NAMES = ['customer', 'date', 'lineorder', 'part', 'supplier']
LOAD_HEADERS = {'lineorder': '-H "columns:lo_orderkey,lo_linenumber,lo_custkey,lo_partkey,lo_suppkey,\
lo_orderdate,lo_orderpriotity,lo_shippriotity,lo_quantity,lo_extendedprice,lo_ordtotalprice,lo_discount,lo_revenue,\
lo_supplycost,lo_tax,lo_commitdate,lo_shipmode,\
lo_partkey_bitmap=to_bitmap(lo_partkey),lo_partkey_hll=HLL_HASH(lo_partkey),lo_year=year(lo_orderdate)"'}

TEST_TABLE_LOAD_COMMAND = 'curl -sS -m 1800 %(header)s --location-trusted -u %(user)s@default_cluster:  -T %(data_file)s \
http://%(host)s:%(http_port)d/api/default_cluster:%(db_name)s/%(table_name)s/_stream_load'

DEFAULT_DOWNLOAD_PATH = os.environ['DOWNLOAD_PATH'] if 'DOWNLOAD_PATH' in os.environ \
    else 'https://s3plus.meituan.net/v1/mss_aee445ae7aa0438c82eacce6e3f6cb2c/doris-functionaltest/ssbsf02/'

SSB_SF02_TAR_NAME = 'ssb_sf02.tar'

RETRIES_COUNT = 5

DEFAULT_FE_QUERY_PORT = os.environ['FE_QUERY_PORT'] if 'FE_QUERY_PORT' in os.environ else 9030
DEFAULT_FE_HTTP_PORT = os.environ['FE_HTTP_PORT'] if 'FE_HTTP_PORT' in os.environ else 8030


def download_with_progress(url, path):
    print("Downloading from %s to temp path %s" % (url, path))
    for i in range(RETRIES_COUNT):
        try:
            with open(path, 'wb') as f:
                response = requests.get(url, stream=True)
                response.raise_for_status()
                total_length = response.headers.get('content-length')
                if total_length is None or int(total_length) == 0:
                    print("No content-length, will download file without progress")
                    f.write(response.content)
                else:
                    dl = 0
                    total_length = int(total_length)
                    print("Content length is %ld bytes" % total_length)
                    for data in response.iter_content(chunk_size=4096):
                        dl += len(data)
                        f.write(data)
                        if sys.stdout.isatty():
                            done = int(50 * dl / total_length)
                            percent = int(100 * float(dl) / total_length)
                            sys.stdout.write("\r[{}{}] {}%".format('=' * done, ' ' * (50-done), percent))
                            sys.stdout.flush()
            break
        except Exception as ex:
            sys.stdout.write("\n")
            time.sleep(3)
            print("Exception while downloading %s, retry %s" % (ex, i + 1))
            if os.path.exists(path):
                os.remove(path)
    else:
        raise Exception("Cannot download dataset from {}, all retries exceeded".format(url))

    sys.stdout.write("\n")
    sys.stdout.write("Downloading finished")


if __name__ == '__main__':
    parser = ArgumentParser(description='Prepare doris')

    parser.add_argument('--host', default='localhost', help='Doris host')
    parser.add_argument('--port', type=int, default=DEFAULT_FE_QUERY_PORT, help='Doris port')
    parser.add_argument('--user', default='root', help='Test user')
    parser.add_argument('--password', default='', help='Test user password')
    parser.add_argument('--database', default=TEST_DB_NAME, help='Test database')
    parser.add_argument('--http-port', type=int, default=DEFAULT_FE_HTTP_PORT)
    parser.add_argument('--base-url', default=DEFAULT_DOWNLOAD_PATH, help='download url prefix')

    parser.add_argument('--data-path', required=True, help='data_prepare path')

    args = parser.parse_args()

    pattern = re.compile(r'^create_(.+)\.sql$')

    if not os.path.exists(args.data_path):
        os.makedirs(args.data_path)
    save_file_name = os.path.join(args.data_path, SSB_SF02_TAR_NAME)
    if not os.path.exists(save_file_name):
        download_url = os.path.join(args.base_url, SSB_SF02_TAR_NAME)
        download_with_progress(download_url, save_file_name)
        with tarfile.open(save_file_name, 'r') as comp_file:
            comp_file.extractall(path=args.data_path)

    table2create_sql = {}
    for file_name in os.listdir(args.data_path):
        obj = re.search(pattern, file_name)
        if obj:
            table_name = obj.group(1)
            if table_name in TEST_TABLE_NAMES:
                table2create_sql[table_name] = os.path.join(args.data_path, file_name)

    for table_name in TEST_TABLE_NAMES:
        if table_name not in table2create_sql:
            print(f"Can not find table({table_name})'s create sql", file=sys.stderr)
            sys.exit(1)

    connection = pymysql.connect(host=args.host,
                                 port=args.port,
                                 user=args.user,
                                 password=args.password)

    with connection:
        with connection.cursor() as cursor:
            cursor.execute('CREATE DATABASE IF NOT EXISTS ' + args.database)

    connection = pymysql.connect(host=args.host,
                                 port=args.port,
                                 user=args.user,
                                 password=args.password,
                                 database=args.database)

    with connection.cursor() as cursor:
        for table_name in TEST_TABLE_NAMES:
            with open(table2create_sql[table_name]) as sql_file_h:
                sql = sql_file_h.read()
                cursor.execute(sql)

    def test_data_ready():
        cursor.execute(f"SELECT COUNT(1) AS cnt FROM {table_name}")
        result = cursor.fetchall()
        return result and len(result) > 0 and result[0][0] > 0

    load_common_param_env = {'user': args.user, 'host': args.host, 'http_port': args.http_port, 'db_name': args.database}

    with connection:
        with connection.cursor() as cursor:
            for table_name in TEST_TABLE_NAMES:
                if test_data_ready():
                    print(f"table({table_name})'s data is ready")
                else:
                    env = copy.deepcopy(load_common_param_env)
                    env['header'] = '' if table_name not in LOAD_HEADERS else LOAD_HEADERS[table_name]
                    env['table_name'] = table_name
                    data_file_name = table_name + '.csv'
                    env['data_file'] = os.path.join(args.data_path, data_file_name)
                    command = TEST_TABLE_LOAD_COMMAND % env
                    print(command)
                    proc_res = subprocess.call(command, stdout=PIPE, shell=True)
                    if proc_res:
                        print(f"Load data to table({table_name}) fail", file=sys.stderr)
                        sys.exit(1)
                    else:
                        print(f'Load data to table({table_name}) success')