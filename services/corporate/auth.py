"""Reuse the Open Close Map scoped GitHub OIDC pattern with separate identity."""
from functools import lru_cache
import jwt
from fastapi import HTTPException
ISSUER='https://token.actions.githubusercontent.com'
AUDIENCE='https://buzz-now-1.onrender.com/corporate'
REPO='yamamotoyama2811365-pixel/-corporate-signal'
REPO_ID='1363535163'
WORKFLOW=REPO+'/.github/workflows/collect.yml@refs/heads/main'
BACKUP_REPO='yamamotoyama2811365-pixel/buzz-now'
BACKUP_REPO_ID='1351407859'
BACKUP_WORKFLOW=BACKUP_REPO+'/.github/workflows/corporate-collection-backup.yml@refs/heads/main'

def allowed(c):
    event_ok=c.get('event_name') in {'schedule','workflow_dispatch','push'}
    ref_ok=c.get('ref')=='refs/heads/main'
    if not (event_ok and ref_ok):
        return False
    repo=str(c.get('repository') or '')
    repo_id=str(c.get('repository_id') or '')
    workflow_ref=str(c.get('workflow_ref') or '')
    sub=str(c.get('sub') or '')
    if repo==REPO and repo_id==REPO_ID and workflow_ref==WORKFLOW:
        subjects={'repo:'+REPO+':ref:refs/heads/main','repo:yamamotoyama2811365-pixel@321271816/-corporate-signal@'+REPO_ID+':ref:refs/heads/main'}
        return sub in subjects
    if repo==BACKUP_REPO and repo_id==BACKUP_REPO_ID and workflow_ref==BACKUP_WORKFLOW:
        return sub=='repo:'+BACKUP_REPO+':ref:refs/heads/main'
    return False
@lru_cache(maxsize=1)
def jwks(): return jwt.PyJWKClient(ISSUER+'/.well-known/jwks',cache_jwk_set=True,lifespan=300,timeout=5)
def authorize(request):
    h=request.headers.get('authorization','')
    if not h.startswith('Bearer ') or len(h)>16384: raise HTTPException(401,'Authentication required')
    try:
        token=h[7:]; key=jwks().get_signing_key_from_jwt(token).key
        claims=jwt.decode(token,key,algorithms=['RS256'],issuer=ISSUER,audience=AUDIENCE,options={'require':['exp','iat','nbf','sub','repository','repository_id','ref','workflow_ref','event_name']})
    except (jwt.PyJWTError,ValueError): raise HTTPException(401,'Invalid identity')
    if not allowed(claims): raise HTTPException(403,'Workflow not authorized')
