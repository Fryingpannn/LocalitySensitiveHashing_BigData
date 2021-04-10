import sys
import os

def pretty_print_bands(rdd):
    out_string = ""
    # rdd contains elements in the form ((band_id, bucket_id), [ state_names ])
    for x in sorted(rdd.collect()):
        if len(x[1]) < 2:
            continue # don't print buckets with a single element
        out_string += ("------------band {}, bucket {}-------------\n"
                               .format(x[0][0],x[0][1]))
        for y in sorted(x[1]):
            out_string += y + " "
        out_string += os.linesep

    return out_string
