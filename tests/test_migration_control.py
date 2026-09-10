import asyncio
import unittest
from app.migration_control import MigrationMaintenance, migration_settings


class MigrationTests(unittest.TestCase):
    def test_select_and_rollback_without_replacing_source_secret(self):
        env = {'DATABASE_URL': 'source', 'NEON_DATABASE_URL': 'target'}
        self.assertEqual(migration_settings(env), (False, 'source', 'source'))
        env['DATABASE_BACKEND'] = 'neon'
        self.assertEqual(migration_settings(env), (False, 'neon', 'target'))
        env['DATABASE_BACKEND'] = 'source'
        self.assertEqual(migration_settings(env)[2], 'source')

    def test_fail_closed_for_missing_neon_or_invalid_selection(self):
        for env in ({'DATABASE_BACKEND': 'neon'}, {'DATABASE_BACKEND': 'typo'}):
            with self.assertRaises(ValueError):
                migration_settings(env)

    def test_pause_blocks_write_like_gets_posts_and_websockets(self):
        for kind, path, method in [('http', '/api/collect-now-browser', 'GET'),
                                   ('http', '/api/trends', 'POST'),
                                   ('http', '/', 'GET'), ('websocket', '/ws', 'GET')]:
            messages = []
            async def app(*args):
                self.fail('Maintenance must not invoke application or background jobs')
            async def send(message):
                messages.append(message)
            asyncio.run(MigrationMaintenance(app, True)(
                {'type': kind, 'path': path, 'method': method}, None, send))
            self.assertEqual(messages[0].get('status', messages[0].get('code')), 503 if kind == 'http' else 1013)

    def test_health_lifespan_and_normal_mode_pass_through(self):
        for paused, scope in [(True, {'type': 'http', 'path': '/health'}),
                              (True, {'type': 'lifespan'}),
                              (False, {'type': 'http', 'path': '/'})]:
            calls = []
            async def app(*args):
                calls.append(True)
            asyncio.run(MigrationMaintenance(app, paused)(scope, None, None))
            self.assertEqual(calls, [True])


if __name__ == '__main__':
    unittest.main()
