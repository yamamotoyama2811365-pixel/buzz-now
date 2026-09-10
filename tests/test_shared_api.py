import os
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import FastAPI
from fastapi.testclient import TestClient
import jwt
from services.open_close import auth


class IdentityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.key=rsa.generate_private_key(public_exponent=65537,key_size=2048)

    def claims(self, **overrides):
        now=int(time.time())
        c=dict(iss=auth.ISSUER,aud=auth.AUDIENCE,sub='repo:'+auth.REPOSITORY+':ref:refs/heads/main',
               repository=auth.REPOSITORY,repository_id=auth.REPOSITORY_ID,ref='refs/heads/main',
               workflow_ref=auth.COLLECTOR,event_name='schedule',iat=now,nbf=now-1,exp=now+300)
        c.update(overrides);return c

    def verify(self, claims, method='GET', path='/api/source-health', signer=None):
        token=jwt.encode(claims,signer or self.key,algorithm='RS256')
        request=SimpleNamespace(headers={'authorization':'Bearer '+token},method=method,url=SimpleNamespace(path='/open-close'+path))
        fake=SimpleNamespace(get_signing_key_from_jwt=lambda token:SimpleNamespace(key=self.key.public_key()))
        with patch.object(auth,'jwks',return_value=fake): return auth.require_operator(request)

    def test_production_workflow_has_only_collector_permissions(self):
        self.assertTrue(self.verify(self.claims()))
        self.assertTrue(self.verify(self.claims(), 'POST','/api/full-cycle'))
        for method,path in [('GET','/api/admin/inquiries'),('POST','/api/hub-repair-reset')]:
            with self.assertRaises(Exception) as ex:self.verify(self.claims(),method,path)
            self.assertEqual(ex.exception.status_code,403)

    def test_invalid_identity_or_signature_is_rejected(self):
        for changes in [dict(aud='other'),dict(iss='https://evil.invalid'),dict(exp=int(time.time())-10),dict(repository_id='1'),dict(event_name='pull_request'),dict(ref='refs/heads/other'),dict(workflow_ref='other')]:
            with self.assertRaises(Exception):self.verify(self.claims(**changes))
        with self.assertRaises(Exception):self.verify(self.claims(),signer=rsa.generate_private_key(public_exponent=65537,key_size=2048))

    def test_immutable_github_subject_format(self):
        c=self.claims(sub='repo:'+auth.IMMUTABLE_REPOSITORY+':ref:refs/heads/main')
        self.assertTrue(self.verify(c))
        c['sub']=c['sub'].replace('@321271816','@1')
        with self.assertRaises(Exception):self.verify(c)

    def test_check_workflow_is_read_only(self):
        c=self.claims(ref='refs/heads/ops/shared-api-check',sub='repo:'+auth.REPOSITORY+':ref:refs/heads/ops/shared-api-check',workflow_ref=auth.CHECK,event_name='push')
        self.assertTrue(self.verify(c))
        with self.assertRaises(Exception):self.verify(c,'POST','/api/full-cycle')

    def test_no_key_never_opens_admin_access(self):
        req=SimpleNamespace(headers={},method='GET',url=SimpleNamespace(path='/open-close/api/admin/inquiries'))
        with self.assertRaises(Exception) as ex: auth.require_operator(req)
        self.assertEqual(ex.exception.status_code,401)


class MountTest(unittest.TestCase):
    def test_mounted_routes_cors_and_database_isolation(self):
        import importlib
        with patch.dict(os.environ,{'DATABASE_URL':'postgresql://wrong/buzz','OPEN_CLOSE_DATABASE_URL':'postgresql://right/stores'}):
            from services.open_close import main
            main=importlib.reload(main)
        self.assertEqual(main.DATABASE_URL,'postgresql://right/stores')
        root=FastAPI();root.mount('/open-close',main.app)
        with patch.object(main,'db_conn',side_effect=AssertionError('Unexpected DB access')):
            with TestClient(root) as client:
                h=client.get('/open-close/health');self.assertEqual(h.status_code,200)
                self.assertEqual(h.json()['hosting'],'shared')
                self.assertEqual(client.get('/open-close/api/admin/inquiries').status_code,401)
                self.assertEqual(client.post('/open-close/api/full-cycle').status_code,401)
                r=client.options('/open-close/api/stores',headers={'Origin':'https://open-close-map.onrender.com','Access-Control-Request-Method':'GET'})
                self.assertEqual(r.status_code,200)
                self.assertEqual(r.headers['access-control-allow-origin'],'https://open-close-map.onrender.com')

if __name__=='__main__':unittest.main()
