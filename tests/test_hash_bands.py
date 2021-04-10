import sys
sys.path.insert(0, './answers')
from answer import hash_bands

def compare(r1, r2):
    if len(r1) != len(r2):
        print(str(len(r1)) + " - " + str(len(r2)))
    assert(len(r1) == len(r2))
    for i in range(len(r1)):
        list_found = False
        for j in range(len(r2)):
            res_list = r1[i]
            a_list = r2[j]
            if res_list == a_list:
                list_found = True
                break
        if not list_found:
            print(r2[j])
        assert(list_found)
    return True
def test_hash_bands():
    res = {0: [['oh','on','qc'], ['ga','nc','sc','tn'], ['nf','ns'], ['ct','nj']],
    1: [['md','nj','pa','va'], ['on','qc'], ['nb','ns'], ['id','mt'], ['ia','mn'], ['mi','oh','wv'], ['ar','la'], ['il','mo','wi'],['nc','tn'], ['ne','sd'], ['ct','ma','me','nh','ny']],
    2: [['nb','qc'], ['md','pa','sc','va'], ['ak','yt'], ['id','wa'], ['de','nj','ri'], ['me','nh'], ['ar','tn'], ['il','nc']],
    3: [['tn','va'], ['ar','ms'], ['al','ga','ky','sc'], ['il','mo'], ['ct','ma','nh'], ['on','qc'], ['mt','wy'], ['mn','wi'], ['me','nb']],
    4: [['ma','tn','va','wv'], ['nt','yt'], ['nb','ns']]}
    out = hash_bands("./data/plants.data", 123, 5, 7)
    for i in range(5):
        compare(res[i], out[i])
