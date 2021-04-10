import sys
sys.path.insert(0, './answers')
from answer import signatures as sig

def test_signatures():
    result=sig("./data/plants.data", 123, 10, "qc")
    assert(result=={int(line.split(': ')[0]): int(line.strip().split(': ')[1])
                       for line in open("tests/test-signatures.txt","r")})
