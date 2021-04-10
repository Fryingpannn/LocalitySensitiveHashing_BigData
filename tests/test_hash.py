import sys
sys.path.insert(0, './answers')
from answer import hash_plants as hsh

def test_hash():
    assert(hsh(123, 12, 7, 99)==6)
