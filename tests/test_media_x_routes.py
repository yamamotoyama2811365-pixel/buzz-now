import unittest
from app.media_x_routes import config, status, validate_channel, send_verified


class RoutingTests(unittest.TestCase):
    def setUp(self):
        self.env = {'BUFFER_API_KEY': 'existing-test-key', 'BUFFER_MEDIA_API_KEY': 'new-test-key',
                    'BIYO_X_ENABLED': 'true', 'BIYO_X_BUFFER_CHANNEL_ID': 'beauty-channel',
                    'TADAGE_X_ENABLED': 'true', 'TADAGE_X_BUFFER_CHANNEL_ID': 'game-channel',
                    'TADAGE_X_HANDLE': 'test_game'}
        self.channel = {'id': 'game-channel', 'name': 'test_game', 'service': 'twitter',
                        'isDisconnected': False, 'isLocked': False, 'isQueuePaused': False}

    def test_new_account_never_falls_back_to_existing_credentials(self):
        self.env.pop('BUFFER_MEDIA_API_KEY')
        self.assertEqual(config('tadage', self.env)['api_key'], '')
        self.assertFalse(validate_channel('tadage', self.channel, self.env))
        self.assertEqual(config('biyo', self.env)['api_key'], 'existing-test-key')

    def test_wrong_or_disconnected_account_is_rejected(self):
        self.assertTrue(validate_channel('tadage', self.channel, self.env))
        for key, value in [('name', '7d2sz_biyo'), ('service', 'threads'), ('isDisconnected', True), ('isLocked', True)]:
            self.assertFalse(validate_channel('tadage', dict(self.channel, **{key: value}), self.env))
        self.env['TADAGE_X_BUFFER_CHANNEL_ID'] = 'beauty-channel'
        self.assertFalse(validate_channel('tadage', dict(self.channel, id='beauty-channel'), self.env))

    def test_reserved_existing_account_cannot_be_used(self):
        for channel_id in ('6a9a680a065799be4686e3d9', '6aa41ee3cd8b9c702c4e120d', '6aa172decd8b9c702c382ea4'):
            self.env['TADAGE_X_BUFFER_CHANNEL_ID'] = channel_id
            self.assertFalse(validate_channel('tadage', dict(self.channel, id=channel_id), self.env))

    def test_status_contains_no_credential_values(self):
        result = repr(status(self.env))
        self.assertNotIn('existing-test-key', result)
        self.assertNotIn('new-test-key', result)

    def test_new_account_uses_its_own_key_for_query_and_send(self):
        requests = []
        channel = self.channel
        class Response:
            status_code = 200
            def __init__(self, data):
                self.data = data
            def json(self):
                return {'data': self.data}
        def post(url, **kwargs):
            requests.append(kwargs)
            if len(requests) == 1:
                return Response({'channel': channel})
            return Response({'createPost': {'post': {'id': 'test-receipt', 'status': 'pending'}}})
        def wrong_transport(_):
            raise AssertionError('new account used existing Buffer transport')
        result = send_verified('tadage', '無料ゲーム記事のご案内', wrong_transport, self.env, post)
        self.assertTrue(result['ok'])
        self.assertFalse(result['publication_verified'])
        self.assertEqual(len(requests), 2)
        self.assertTrue(all(r['headers']['Authorization'] == 'Bearer new-test-key' for r in requests))

    def test_invalid_channel_never_submits(self):
        calls = []
        def graphql(document):
            calls.append(document)
            return {'ok': True, 'data': {'channel': self.channel}}
        result = send_verified('biyo', '美容記事のご案内', graphql, self.env)
        self.assertFalse(result['attempted'])
        self.assertEqual(len(calls), 1)

    def test_unknown_submission_is_not_retried(self):
        calls = []
        def graphql(document):
            calls.append(document)
            if len(calls) == 1:
                return {'ok': True, 'data': {'channel': dict(self.channel, id='beauty-channel', name='7d2sz_biyo')}}
            return {'ok': False, 'reason': 'timeout'}
        result = send_verified('biyo', '美容記事のご案内', graphql, self.env)
        self.assertTrue(result['attempted'])
        self.assertFalse(result['ok'])
        self.assertEqual(len(calls), 2)


if __name__ == '__main__':
    unittest.main()
