#!/usr/bin/env python3
"""Run DDL deployment CLI regressions with a fake mysql; no database required."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name('scanlogs-index-ddl.sh').resolve()
MYSQL = r'''#!/usr/bin/env python3
import json, os, pathlib, re, sys
root = pathlib.Path(os.environ['MOCK_ROOT'])
args = sys.argv[1:]
sql = next(a[len('--execute='):] for a in args if a.startswith('--execute='))
with (root / 'calls').open('a') as f:
    f.write(json.dumps(args) + '\n')
state = json.loads((root / 'state').read_text())
if sql.startswith('SET SESSION'):
    assert '--skip-force' in args
    assert re.match(r'SET SESSION lock_wait_timeout = [0-9]+;\nALTER TABLE ', sql)
    table = re.search(r'ALTER TABLE `([^`]+)`', sql)[1]
    state['attempts'] += 1
    (root / 'state').write_text(json.dumps(state))
    scenario = os.environ['MOCK_SCENARIO']
    if scenario == 'set_error':
        print('ERROR 1231 (42000) at line 1: invalid session value', file=sys.stderr)
        sys.exit(1)
    if scenario == 'error':
        print('ERROR 1061 (42000) at line 2: duplicate index', file=sys.stderr)
        sys.exit(1)
    if scenario == 'abort':
        sys.exit(134)
    if scenario == 'timeout' or (scenario == 'recover' and state['attempts'] <= 2):
        print('ERROR 1205 (HY000) at line 2: Lock wait timeout exceeded; try restarting transaction', file=sys.stderr)
        sys.exit(1)
    if 'ADD INDEX' in sql:
        state['new'].append(table)
    else:
        state['old'].remove(table)
    (root / 'state').write_text(json.dumps(state))
elif sql == 'SELECT DATABASE()':
    print('test_db')
elif sql == 'SELECT VERSION()':
    print('8.0.mock')
elif 'information_schema.tables' in sql:
    print('bn_partitions\nlogs_0\nlogs_1')
elif 'information_schema.statistics' in sql:
    for t in state['old']:
        print(t + '\tidx_bn\t1|BTREE|bn|0|A')
    for t in state['new']:
        print(t + '\tidx_bn_li\t1|BTREE|bn,log_index|0|A,A')
elif sql.startswith('SELECT entity, pi'):
    print('logs\t0\nlogs\t1')
elif sql.startswith('SELECT EXISTS'):
    print('0')
else:
    raise AssertionError(sql)
'''


class DDLTest(unittest.TestCase):
    def run_cli(self, mode='add', scenario='success', extra=()):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            mock = root / 'mysql'
            mock.write_text(MYSQL)
            mock.chmod(0o755)
            sleep = root / 'sleep'
            sleep.write_text('#!/bin/sh\nprintf "%s\\n" "$1" >> "$MOCK_ROOT/sleeps"\n')
            sleep.chmod(0o755)
            (root / 'state').write_text(json.dumps({
                'attempts': 0, 'old': ['logs_0', 'logs_1'],
                'new': ['logs_0', 'logs_1'] if mode == 'drop' else [],
            }))
            env = dict(os.environ, MOCK_ROOT=tmp, MOCK_SCENARIO=scenario,
                       PATH=tmp + os.pathsep + os.environ['PATH'])
            command = ['bash', str(SCRIPT), '--database', 'test_db',
                       '--address-partitions', '0', '--topic-partitions', '0',
                       '--host', 'mock', '--port', '3306', '--user', 'mock',
                       '--mysql-bin', str(mock), '--mode', mode]
            if mode in ('add', 'drop'):
                command.append('--execute')
            result = subprocess.run(command + list(extra), env=env, text=True,
                                    capture_output=True, timeout=15)
            calls = [json.loads(line) for line in (root / 'calls').read_text().splitlines()] if (root / 'calls').exists() else []
            ddls = [next(a[10:] for a in call if a.startswith('--execute='))
                    for call in calls if any(a.startswith('--execute=SET SESSION') for a in call)]
            sleeps = (root / 'sleeps').read_text().splitlines() if (root / 'sleeps').exists() else []
            return result, ddls, sleeps

    def test_success_and_recovery(self):
        for mode in ('add', 'drop'):
            for scenario, count in (('success', 2), ('recover', 4)):
                with self.subTest(mode=mode, scenario=scenario):
                    result, ddls, sleeps = self.run_cli(mode, scenario)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(len(ddls), count)
                    self.assertTrue(all(s.startswith('SET SESSION lock_wait_timeout = 10;\n') for s in ddls))
                    self.assertEqual(sleeps, ['10'] * (count - 2))

    def test_exhaustion_stops_before_next_table(self):
        for mode in ('add', 'drop'):
            result, ddls, sleeps = self.run_cli(mode, 'timeout')
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(len(ddls), 5)
            self.assertTrue(all('`logs_0`' in s for s in ddls))
            self.assertEqual(sleeps, ['10'] * 4)
            self.assertIn('attempt 5/5', result.stderr)

    def test_non_timeout_errors_stop_immediately(self):
        for mode in ('add', 'drop'):
            for scenario in ('set_error', 'error', 'abort'):
                with self.subTest(mode=mode, scenario=scenario):
                    result, ddls, sleeps = self.run_cli(mode, scenario)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertEqual(len(ddls), 1)
                    self.assertEqual(sleeps, [])

    def test_custom_options(self):
        result, ddls, sleeps = self.run_cli(scenario='timeout', extra=(
            '--lock-wait-timeout', '7', '--ddl-max-attempts', '2', '--ddl-retry-interval', '3'))
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len(ddls), 2)
        self.assertTrue(all(s.startswith('SET SESSION lock_wait_timeout = 7;\n') for s in ddls))
        self.assertEqual(sleeps, ['3'])
        result, ddls, sleeps = self.run_cli(extra=('--pause-seconds', '4', '--ddl-retry-interval', '0'))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(sleeps, ['4', '4'])
        result, ddls, sleeps = self.run_cli(scenario='timeout', extra=('--ddl-max-attempts', '1'))
        self.assertEqual(len(ddls), 1)
        self.assertEqual(sleeps, [])

    def test_invalid_options(self):
        for option in ('--lock-wait-timeout', '--ddl-max-attempts', '--ddl-retry-interval'):
            for value in ('-1', 'abc', '1.5', '', '08', '1000000000'):
                result, ddls, _ = self.run_cli(extra=(option, value))
                self.assertEqual(result.returncode, 2, (option, value, result.stderr))
                self.assertEqual(ddls, [])
        for option in ('--lock-wait-timeout', '--ddl-max-attempts'):
            result, _, _ = self.run_cli(extra=(option, '0'))
            self.assertEqual(result.returncode, 2)

    def test_plan_prints_timeout_for_add_and_drop(self):
        result, ddls, sleeps = self.run_cli(mode='plan', extra=('--lock-wait-timeout', '7'))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(ddls, [])
        self.assertEqual(sleeps, [])
        self.assertEqual(result.stdout.count('SET SESSION lock_wait_timeout = 7;'), 4)
        self.assertIn('ADD INDEX', result.stdout)
        self.assertIn('DROP INDEX', result.stdout)


if __name__ == '__main__':
    unittest.main()
