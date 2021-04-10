import sys
sys.path.insert(0, './answers')
from answer import primes

def test_primes():
    assert(primes(50, 41)==
            [int(el.strip()) for el in open("tests/test-primes.txt","r")])
