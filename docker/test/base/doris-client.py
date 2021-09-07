#!/usr/bin/env python3
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

import sys
import os
import copy
from argparse import ArgumentParser
from argparse import ZERO_OR_MORE

import pymysql
from pymysql.cursors import DictCursor
from pymysql import ProgrammingError
from datetime import datetime
import time

DEFAULT_FE_QUERY_PORT = os.environ['FE_QUERY_PORT'] if 'FE_QUERY_PORT' in os.environ else 9030
DEFAULT_BE_HEART_BEAT_PORT = os.environ['BE_HEART_BEAT_PORT'] if 'BE_HEART_BEAT_PORT' in os.environ else 9050


def create_connection(args):
    local_args = copy.deepcopy(args)
    return pymysql.connect(host=local_args.host,
                           port=local_args.port,
                           user=local_args.user,
                           password=local_args.password,
                           database=local_args.database,
                           connect_timeout=5,
                           cursorclass=DictCursor)


def fe_check(args):
    start_time = datetime.now()
    while True:
        try:
            args.sql = 'SHOW CLUSTERS'
            fe_run_sql(args)
            print("Great, connect to fe successfully!")
            return
        except Exception as e:
            if (datetime.now() - start_time).total_seconds() >= args.timeout:
                raise e


def fe_add_backend(args):
    args.sql = 'ALTER SYSTEM ADD BACKEND "{}:{}"'.format(args.be_host, args.be_port)
    try:
        fe_run_sql(args)
        print("Great, fe_add_backend successfully!")
    except ProgrammingError as e:
        if "Same backend already exists" in str(e):
            print(args.be_host + " already exists.")
            return
        else:
            raise e


def fe_del_backend(args):
    args.sql = 'ALTER SYSTEM DROPP BACKEND "{}:{}"'.format(args.be_host, args.be_port)
    fe_run_sql(args)


def check_backends(args):
    args.sql = 'show backends'
    bes = set(args.bes)
    result = fe_run_sql(args)

    total_time = args.wait
    sleep_time = min(total_time / 3, 60)
    if not bes:
        bes = [be['BackendId'] for be in result]
    start_time = datetime.now()
    while True:
        if check_backends0(result, bes):
            print("Great, check_backends successfully!")
            return
        elif (datetime.now() - start_time).total_seconds() >= args.wait:
            raise Exception("check fail")
        time.sleep(sleep_time)
        result = fe_run_sql(args)


def check_backends0(result, bes):
    bes = copy.deepcopy(bes)
    print(f"Check {bes} ")
    for r in result:
        if r['BackendId'] in bes:
            if 'true' == r['Alive']:
                try:
                    total_c = float(r['TotalCapacity'])
                    if total_c == 0:
                        print(f"{r['BackendId']} is new and does not report")
                        return False
                except ValueError:
                    bes.remove(r['BackendId'])
            else:
                print(r)
                return False
    return False if bes else True


def fe_run_sql(args):
    connection = create_connection(args)
    with connection:
        with connection.cursor() as cursor:
            sql = args.sql
            if hasattr(args, 'file') and args.file:
                with open(args.file) as sql_file:
                    sql = sql_file.read()
            cursor.execute(sql)
            result = cursor.fetchall()
            if hasattr(args, 'print_result') and args.print_result:
                print(result)
            return result


def add_fe_connection_arg(parser):
    parser.add_argument('--host', default='localhost', help='Doris port')
    parser.add_argument('--port', type=int, default=DEFAULT_FE_QUERY_PORT, help='Doris port')
    parser.add_argument('--user', default='root', help='Test user')
    parser.add_argument('--password', default='', help='Test user password')
    parser.add_argument('--database', default='', help='Test database')


if __name__ == '__main__':
    parser = ArgumentParser(description='Doris Client')
    sub_parsers = parser.add_subparsers(help="Doris tool")

    fe_parser = sub_parsers.add_parser('fe', help='')
    add_fe_connection_arg(fe_parser)
    fe_parsers = fe_parser.add_subparsers(help='Fe tool')

    fe_init_parser = fe_parsers.add_parser('check')
    fe_init_parser.add_argument('--timeout', type=int, default=60, help='timeout')
    fe_init_parser.set_defaults(handler=fe_check)

    fe_add_backend_parser = fe_parsers.add_parser('add_backend')
    fe_add_backend_parser.add_argument("--be_host", help='be host', required=True)
    fe_add_backend_parser.add_argument("--be_port", type=int, help='be port', default=DEFAULT_BE_HEART_BEAT_PORT)
    fe_add_backend_parser.set_defaults(handler=fe_add_backend)

    fe_del_backend_parser = fe_parsers.add_parser('del_backend')
    fe_del_backend_parser.add_argument("--be_host", help='be host')
    fe_del_backend_parser.add_argument("--be_port", type=int, help='be port', default=DEFAULT_BE_HEART_BEAT_PORT)
    fe_del_backend_parser.set_defaults(handler=fe_del_backend)

    check_backends_parser = fe_parsers.add_parser('check_backends')
    check_backends_parser.add_argument("--bes", nargs=ZERO_OR_MORE, help='be ids', default=[])
    check_backends_parser.add_argument("--wait", type=int, default=120, help='wait timeout')
    check_backends_parser.set_defaults(handler=check_backends)

    fe_run_sql_parser = fe_parsers.add_parser('run_sql')
    fe_run_sql_parser.add_argument("-e", "--sql", help='sql')
    fe_run_sql_parser.add_argument("-f", "--file", help='sql file')
    fe_run_sql_parser.add_argument("--print-result", action='store_true', default=False, help='print result in stdout')
    fe_run_sql_parser.set_defaults(handler=fe_run_sql)

    args = parser.parse_args(sys.argv[1:])
    args.handler(args)
