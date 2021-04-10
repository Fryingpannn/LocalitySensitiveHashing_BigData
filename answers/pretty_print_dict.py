def pretty_print_dict(dic):
  keys = [k for k in dic.keys() if k!='state']
  for k in sorted(keys):
     print("{}: {}".format(k, dic[k]))
