import functools
from hmac import compare_digest
from flask import request
import base64

def is_valid(api_key):
    target = '123456789'
    if target and compare_digest(target, api_key):
        return True

def api_required(func):
    @functools.wraps(func)
    def decorator(*args, **kwargs):
        if request.json:
            api_key = request.json.get("api_key")
        else:
            return {"message": "Please provide an API key"}, 400
        # Check if API key is correct and valid
        if request.method == "POST" and is_valid(api_key):
            return func(*args, **kwargs)
        else:
            return {"message": "The provided API key is not valid"}, 403
    return decorator

def generate_auth_token():
    token = '123456'
    # bytes = token.encode('ascii')
    # message_bytes = base64.b64decode(bytes)
    return token