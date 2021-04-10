import sys
sys.path.insert(0, './answers')
from answer import hash_band

def test_hash_band():
    output = hash_band("./data/plants.data", 123, "qc", 12, 2, 2)
    assert(output==-6464292454374400732 or output==-8417330518760795669
           or output==4911703153322429417)
