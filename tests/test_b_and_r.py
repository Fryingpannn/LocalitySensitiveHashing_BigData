import sys
sys.path.insert(0, './answers')
from answer import get_b_and_r as br


def test_b_and_r():
    out = br(100, 0.8)
    assert(out==open("tests/test-get-b-and-r.txt","r").read())
