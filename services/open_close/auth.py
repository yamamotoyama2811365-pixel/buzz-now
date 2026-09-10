"""Short-lived GitHub identity for the named collector workflow only."""
import hmac
from functools import lru_cache
import jwt
from fastapi import HTTPException

ISSUER = 'https://token.actions.githubusercontent.com'
AUDIENCE = 'https://buzz-now-1.onrender.com/open-close'
REPOSITORY = 'yamamotoyama2811365-pixel/open-close-map'
REPOSITORY_ID = '1359678678'
IMMUTABLE_REPOSITORY = 'yamamotoyama2811365-pixel@321271816/open-close-map@1359678678'
COLLECTOR = REPOSITORY + '/.github/workflows/open-close-map-auto.yml@refs/heads/main'
CHECK = REPOSITORY + '/.github/workflows/shared-api-check.yml@refs/heads/ops/shared-api-check'
READ_ROUTES = {('GET','/api/source-health'), ('GET','/api/prefecture-coverage')}

@lru_cache(maxsize=1)
def jwks():
    return jwt.PyJWKClient(ISSUER+'/.well-known/jwks', cache_jwk_set=True, lifespan=300, timeout=5)


def allowed_claims(claims, method, path):
    if claims.get('repository') != REPOSITORY or str(claims.get('repository_id')) != REPOSITORY_ID:
        return False
    ref=claims.get('ref')
    subjects = {'repo:'+name+':ref:'+str(ref) for name in (REPOSITORY, IMMUTABLE_REPOSITORY)}
    if claims.get('sub') not in subjects:
        return False
    workflow=claims.get('workflow_ref')
    if workflow == COLLECTOR and ref == 'refs/heads/main':
        return claims.get('event_name') in {'schedule','workflow_dispatch'} and (method,path) in READ_ROUTES | {('POST','/api/full-cycle')}
    if workflow == CHECK and ref == 'refs/heads/ops/shared-api-check':
        return claims.get('event_name') == 'push' and (method,path) in READ_ROUTES
    return False


def require_operator(request, admin_key='', supplied_key=None):
    # Optional manual key remains separate from all BUZZ NOW credentials.
    if admin_key and supplied_key and hmac.compare_digest(supplied_key,admin_key):
        return True
    header=request.headers.get('authorization','')
    if not header.startswith('Bearer ') or len(header)>16384:
        raise HTTPException(401,'Operator authentication required')
    token=header[7:]
    try:
        key=jwks().get_signing_key_from_jwt(token).key
        claims=jwt.decode(token,key,algorithms=['RS256'],audience=AUDIENCE,issuer=ISSUER,
                          options={'require':['exp','iat','nbf','iss','aud','sub','repository_id','repository','ref','workflow_ref','event_name']})
    except (jwt.PyJWTError,ValueError):
        raise HTTPException(401,'Invalid operator identity')
    path=request.url.path.removeprefix('/open-close')
    if not allowed_claims(claims,request.method,path):
        raise HTTPException(403,'Workflow or operation not allowed')
    return True
