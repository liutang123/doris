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
import os.path
import re
from datetime import datetime
import subprocess
from subprocess import PIPE
import traceback

from argparse import ArgumentParser
from typing import Tuple, Union
import pymysql.cursors
from pymysql import MySQLError

DEFAULT_FE_QUERY_PORT = os.environ['FE_QUERY_PORT'] if 'FE_QUERY_PORT' in os.environ else 9030


def suite_key_func(item: str) -> Union[int, Tuple[int, str]]:
    if -1 == item.find('_'):
        return 99998, ''

    prefix, suffix = item.split('_', 1)

    try:
        return int(prefix), suffix
    except ValueError:
        return 99997, ''


def tests_in_suite_key_func(item: str) -> int:
    if -1 == item.find('_'):
        return 99998

    prefix, _ = item.split('_', 1)

    try:
        return int(prefix)
    except ValueError:
        return 99997


def removesuffix(text, *suffixes):
    """
    Added in python 3.9
    https://www.python.org/dev/peps/pep-0616/

    This version can work with several possible suffixes
    """
    for suffix in suffixes:
        if suffix and text.endswith(suffix):
            return text[:-len(suffix)]
    return text


def get_reference_file(abs_test_case_file_name):
    """
    Returns reference file name for specified test
    """
    reference_file = removesuffix(abs_test_case_file_name, ".sql") + '.reference'
    return reference_file if os.path.isfile(reference_file) else None


def format_sql_result(result):
    result = map(lambda l: '\t'.join([str(i) for i in l]), result)
    result = '\n'.join(result) + '\n'
    return result


def get_doris_connection():
    return pymysql.connect(host=args.host,
                           port=args.port,
                           user=args.user,
                           password=args.password,
                           database=args.database)


def run_test_suite(suite_dir, suite_tmp_dir):
    failures_total = 0
    passed_total = 0
    all_start_time = datetime.now()
    all_test_case_files = [case_file for case_file in os.listdir(suite_dir) if not case_file.endswith('.reference')]
    all_test_case_files.sort(key=tests_in_suite_key_func)
    test_size = len(all_test_case_files)
    for test_case_file in all_test_case_files:
        total_time = -1
        test_case_name = removesuffix(test_case_file, '.sql')
        absolute_test_case_file = os.path.join(suite_dir, test_case_file)
        reference_file = get_reference_file(absolute_test_case_file)
        stdout_file = os.path.join(suite_tmp_dir, test_case_name) + '.stdout'
        start_time = datetime.now()
        status = "{0:72}".format(test_case_name + ": ")
        try:
            with get_doris_connection() as connection:
                with connection.cursor() as cursor:
                    with open(absolute_test_case_file) as sql_file_h:
                        with open(stdout_file, 'w') as stdout_file_h:
                            sqls = sql_file_h.readlines()
                            for sql in filter(lambda s: s.strip(), sqls):
                                cursor.execute(sql)
                                result = format_sql_result(cursor.fetchall())
                                stdout_file_h.write(result)
            total_time = (datetime.now() - start_time).total_seconds()
            result_is_different = subprocess.call(['diff', '-q', reference_file, stdout_file], stdout=PIPE)
            if result_is_different:
                failures_total += 1
                status += 'FAIL'
                status += " {0:.2f} sec.".format(total_time)
                status += " - result differs with reference:\n"
                if os.path.isfile(stdout_file):
                    status += "result:\n"
                    status += '\n'.join(open(stdout_file).read().split('\n')[:100])
                    status += '\n'
            else:
                passed_total += 1
                status += 'OK'
                status += " {0:.2f} sec.".format(total_time)
                status += '\n'
        except MySQLError as e:
            failures_total += 1
            status += 'FAIL'
            status += " {0:.2f} sec.".format(total_time)
            status += ' - query error {}\n'.format(e)

            if os.path.isfile(stdout_file):
                status += ", result:\n\n"
                status += '\n'.join(
                    open(stdout_file).read().split('\n')[:100])
                status += '\n'
        except:
            exc_type, exc_value, tb = sys.exc_info()
            exc_name = exc_type.__name__
            traceback_str = "\n".join(traceback.format_tb(tb, 10))

            print(f"FAIL - Test internal error: {exc_name}")
            print(f"{exc_value}\n{traceback_str}")

        if os.path.exists(stdout_file):
            # os.remove(stdout_file)
            pass
        if status and not status.endswith('\n'):
            status += '\n'

        sys.stdout.write(status)
        sys.stdout.flush()

    if failures_total > 0:
        print(f"\nHaving {failures_total} errors! {passed_total} tests passed. "
              f"{(datetime.now() - all_start_time).total_seconds():.2f} s elapsed\n")
        sys.exit(1)
    else:
        sys.stdout.write(f"\nAll {passed_total} tests passed. {(datetime.now() - all_start_time).total_seconds():.2f} s"
                         f" elapsed\n")
    sys.stdout.flush()


def is_data_present():
    # TODO llj
    return True


suite_path_pattern = re.compile('^[0-9]+_(.+)$')


def should_run_suite(abs_suite_dir, suite_dir):
    if not re.search(suite_path_pattern, suite_dir):
        print(f"{suite_dir} do not math{suite_path_pattern}")
        return False
    if not os.path.isdir(abs_suite_dir):
        return False
    suite_dir = os.path.basename(suite_dir)
    if 'stateful' in suite_dir:
        if args.no_stateful:
            print("Won't run stateful tests because they were manually disabled.")
            return False
        elif not is_data_present():
            print("Won't run stateful tests because test data wasn't loaded.")
            return False
    elif 'stateless' in suite_dir and args.no_stateless:
        print("Won't run stateless tests because they were manually disabled.")
        return False
    return True


def run_tests():
    base_dir = os.path.abspath(args.queries)
    base_tmp_dir = os.path.abspath(args.tmp)
    if not os.path.exists(base_tmp_dir):
        os.makedirs(base_tmp_dir)

    for suite_dir in sorted(os.listdir(base_dir), key=suite_key_func):
        abs_suite_dir = os.path.join(base_dir, suite_dir)
        if should_run_suite(abs_suite_dir, suite_dir):
            suite_tmp_dir = os.path.join(base_tmp_dir, suite_dir)
            if not os.path.exists(suite_tmp_dir):
                os.makedirs(suite_tmp_dir)
            ori_database = args.database
            if 'stateful' in suite_dir:
                args.database = 'ssb_sf02'
            run_test_suite(abs_suite_dir, suite_tmp_dir)
            if 'stateful' in suite_dir:
                args.database = ori_database


if __name__ == '__main__':
    parser = ArgumentParser(description='Doris functional tests')
    parser.add_argument('-q', '--queries', default='queries', help='Path to queries dir')
    parser.add_argument('--tmp', help='Path to tmp dir')
    parser.add_argument('--host', default='localhost', help='Doris host')
    parser.add_argument('--port', type=int, default=DEFAULT_FE_QUERY_PORT, help='Doris port')
    parser.add_argument('--user', default='root', help='Test user')
    parser.add_argument('--password', default='', help='Test user password')
    parser.add_argument('--database', default='', help='Test database')

    parser.add_argument('--no-stateless', action='store_true', default=False)
    parser.add_argument('--no-stateful', action='store_true', default=False)

    args = parser.parse_args()

    if args.queries and not os.path.isdir(args.queries):
        print(f"Cannot access the specified directory with queries ({os.path.abspath(args.queries)})", file=sys.stderr)
        sys.exit(1)

    if args.tmp is None:
        args.tmp = args.queries + '_tmp'

    print("Run tests from '" + args.queries + "' directory to '" + args.tmp + "' directory")
    run_tests()
