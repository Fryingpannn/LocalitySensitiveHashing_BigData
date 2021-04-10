import sys
sys.path.insert(0, './answers')
from answer import data_preparation as dp

def test_data_preparation():
    a = dp("./data/plants.data", "urtica", "qc")
    assert(a==1)

    a = dp("./data/plants.data", "zinnia maritima", "hi")
    assert(a==1)

    a = dp("./data/plants.data", "tephrosia candida", "az")
    assert(a==0)
