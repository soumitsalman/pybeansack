from datetime import datetime as dt, timedelta, timezone
import math
import tiktoken

_encoding = tiktoken.get_encoding("cl100k_base")

def chunk(input: str, context_len: int) -> list[str]:
    tokens = _encoding.encode(input)
    num_chunks = math.ceil(len(tokens) / context_len)
    chunk_size = math.ceil(len(tokens) / num_chunks)
    return [_encoding.decode(tokens[start : start+chunk_size]) for start in range(0, len(tokens), chunk_size)]

def truncate(input: str, n_ctx) -> str:
    tokens = _encoding.encode(input)
    return _encoding.decode(tokens[:n_ctx]) 

def count_tokens(input: str) -> int:
    return len(_encoding.encode(input))

now = lambda: int(dt.now(tz=timezone.utc).timestamp())
ndays_ago = lambda ndays: int((dt.now(tz=timezone.utc) - timedelta(days=ndays)).timestamp())
