import sys
sys.path.insert(0, './answers')
from answer import hash_list as hl

def test_hash_list():
    assert(hl(123, 150, 10, 7, 32)==61)
