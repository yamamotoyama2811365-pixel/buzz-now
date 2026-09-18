"""Explicit routing for the four new X accounts; no posting until enabled.

Beauty uses the existing Buffer account. The other three sites use a separate
Buffer account and must never fall back to the existing account's credentials.
"""
import json
import os
import re

SITES = {
    'biyo': ('美容医療コンパス', 'existing', 'BIYO', '7d2sz_biyo'),
    'tadage': ('タダゲー手帖', 'new', 'TADAGE', ''),
    'otona': ('大人の恋のすすめ', 'new', 'OTONA', ''),
    'jleague': ('一生Jリーグ', 'new', 'JLEAGUE', 'issho_j'),
}


def config(site, environ=None):
    env = os.environ if environ is None else environ
    name, account, prefix, known_handle = SITES[site]
    key_name = 'BUFFER_API_KEY' if account == 'existing' else 'BUFFER_MEDIA_API_KEY'
    return {
        'name': name, 'account': account, 'key_name': key_name,
        'api_key': env.get(key_name, '').strip(),
        'channel_id': env.get(prefix + '_X_BUFFER_CHANNEL_ID', '').strip(),
        'handle': (known_handle or env.get(prefix + '_X_HANDLE', '')).strip().lstrip('@').lower(),
        'enabled': env.get(prefix + '_X_ENABLED', 'false').lower() == 'true',
    }


def status(environ=None):
    result = {}
    for site in SITES:
        c = config(site, environ)
        result[site] = {
            'name': c['name'], 'buffer_account': c['account'],
            'handle': c['handle'] or None, 'enabled': c['enabled'],
            'credential_configured': bool(c['api_key']),
            'channel_configured': bool(c['channel_id']),
            'ready_to_verify': bool(c['enabled'] and c['api_key'] and c['channel_id'] and c['handle']),
        }
    return {'accounts': result, 'scheduler_started': False,
            'note': 'Configuration only. No automatic publication has been enabled by this module.'}


def validate_channel(site, channel, environ=None):
    env = os.environ if environ is None else environ
    c = config(site, env)
    if not (c['enabled'] and c['api_key'] and c['channel_id'] and c['handle']):
        return False
    reserved = {
        env.get('BUFFER_CHANNEL_ID', '6a9a680a065799be4686e3d9'),
        env.get('CITY_CORP_X_BUFFER_CHANNEL_ID', '6aa41ee3cd8b9c702c4e120d'),
        env.get('BUFFER_THREADS_CHANNEL_ID', '6aa172decd8b9c702c382ea4'),
    }
    other_ids = {config(other, env)['channel_id'] for other in SITES if other != site}
    return (c['channel_id'] not in reserved | other_ids
            and channel.get('id') == c['channel_id']
            and str(channel.get('name', '')).lstrip('@').lower() == c['handle']
            and channel.get('service') == 'twitter'
            and all(channel.get(flag) is False for flag in ('isDisconnected', 'isLocked', 'isQueuePaused')))


def send_verified(site, text, existing_graphql, environ=None, post=None):
    """Send one caller-selected post after verifying its channel. No retries.

    The caller must durably claim/deduplicate its job before invoking this
    function. An unknown delivery result must not be retried automatically.
    """
    c = config(site, environ)
    if not (c['enabled'] and c['api_key'] and c['channel_id'] and c['handle']):
        return {'ok': False, 'reason': 'account_setup_incomplete', 'attempted': False}
    count_text = re.sub(r'https?://\S+', ' ' * 23, text)
    if not text.strip() or sum(1 if ord(ch) < 128 else 2 for ch in count_text) > 280:
        return {'ok': False, 'reason': 'invalid_post_length', 'attempted': False}

    def query(document):
        if c['account'] == 'existing':
            return existing_graphql(document)
        try:
            transport = post
            if transport is None:
                import httpx
                transport = httpx.post
            response = transport('https://api.buffer.com',
                headers={'Authorization': 'Bearer ' + c['api_key'], 'Content-Type': 'application/json'},
                json={'query': document}, timeout=45.0)
            if response.status_code != 200:
                return {'ok': False, 'reason': 'buffer_http_error', 'status_code': response.status_code}
            body = response.json()
            if body.get('errors'):
                return {'ok': False, 'reason': 'buffer_graphql_error'}
            return {'ok': True, 'data': body.get('data') or {}}
        except Exception:
            return {'ok': False, 'reason': 'buffer_result_unknown'}

    verified = query('query MediaXChannel { channel(input: { id: ' + json.dumps(c['channel_id'])
                     + ' }) { id name service isDisconnected isLocked isQueuePaused } }')
    if not verified.get('ok') or not validate_channel(site, (verified.get('data') or {}).get('channel') or {}, environ):
        return {'ok': False, 'reason': 'channel_not_verified', 'attempted': False}
    result = query('mutation MediaXPost { createPost(input: { text: ' + json.dumps(text)
                   + ' channelId: ' + json.dumps(c['channel_id'])
                   + ' schedulingType: automatic mode: shareNow }) {'
                   + ' ... on PostActionSuccess { post { id status } }'
                   + ' ... on MutationError { message } } }')
    receipt = ((result.get('data') or {}).get('createPost') or {}).get('post') or {}
    if result.get('ok') and receipt.get('id'):
        return {'ok': True, 'attempted': True, 'buffer_post_id': receipt['id'],
                'buffer_status': receipt.get('status'), 'publication_verified': False}
    return {'ok': False, 'attempted': True, 'reason': 'submission_failed_or_unknown',
            'publication_verified': False}
