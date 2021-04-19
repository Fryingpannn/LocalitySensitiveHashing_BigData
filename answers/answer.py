from pyspark.sql import SparkSession
from pretty_print_dict import pretty_print_dict as ppd
import random
from scipy.special import lambertw
# Dask imports
import dask.bag as db
import dask.dataframe as df
from pyspark.rdd import RDD
from pyspark.sql import Row
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

all_states = ["ab", "ak", "ar", "az", "ca", "co", "ct", "de", "dc",
              "fl", "ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la",
              "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv",
              "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa",
              "pr", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "vi",
              "wa", "wv", "wi", "wy", "al", "bc", "mb", "nb", "lb", "nf",
              "nt", "ns", "nu", "on", "qc", "sk", "yt", "dengl", "fraspm"]


def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark


def toCSVLineRDD(rdd):
    """This function is used by toCSVLine to convert an RDD into a CSV string

    """
    a = rdd.map(lambda row: ",".join([str(elt) for elt in row]))\
           .reduce(lambda x, y: "\n".join([x, y]))
    return a + "\n"


def toCSVLine(data):
    """This function convert an RDD or a DataFrame into a CSV string

    """
    if isinstance(data, RDD):
        return toCSVLineRDD(data)
    elif isinstance(data, DataFrame):
        return toCSVLineRDD(data.rdd)
    return None

'''
Get all the states and the plants they have.
Returns RDD of items as (name of the state, {dictionary})
'''
def get_dict(plants_df):
  # convert into rdd and split single string into list, first element is the plant
  plants_rdd = plants_df.rdd.map(lambda row: row["value"].split(","))
  # re-order: (id, plant, [states])
  plants_rdd = plants_rdd.map(lambda row: Row(plant=row[0], items=row[1:]))

  # states & plants list
  states_plants = plants_rdd.map(lambda row: (row['plant'], row['items'])).collect()

  # use flat map to separate list returns into individual items. iterate list to create dictionary: (name of the state, {dictionary})
  states_plants_final = plants_rdd.flatMap(lambda row: row[1]).distinct().map(lambda _state: (_state, {row[0]: (1 if _state in row[1] else 0) for row in states_plants}))
  return states_plants_final

def data_preparation(data_file, key, state):
    """Our implementation of LSH will be based on RDDs. As in the clustering
    part of LA3, we will represent each state in the dataset as a dictionary of
    boolean values with an extra key to store the state name.
    We call this dictionary 'state dictionary'.

    Task 1 : Write a script that
             1) Creates an RDD in which every element is a state dictionary
                with the following keys and values:

                    Key     |         Value
                ---------------------------------------------
                    name    | abbreviation of the state
                    <plant> | 1 if <plant> occurs, 0 otherwise

             2) Returns the value associated with key
                <key> in the dictionary corresponding to state <state>

    *** Note: Dask may be used instead of Spark.

    Keyword arguments:
    data_file -- csv file of plant name/states tuples (e.g. ./data/plants.data)
    key -- plant name
    state -- state abbreviation (see: all_states)
    """
    spark = init_spark()
    # read data from file
    plants_df = spark.read.text(data_file)
    # returns (name of the state, {dictionary of bool plants})
    states_plants_final = get_dict(plants_df)
    res = states_plants_final.filter(lambda row: row[0] == state).collect()[0][1][key]

    return res


def isPrime(n):
    # Corner cases
    if (n <= 1):
        return False
    if (n <= 3):
        return True

    # This is checked so that we can skip
    # middle five numbers in below loop
    if (n % 2 == 0 or n % 3 == 0):
        return False

    i = 5
    while (i * i <= n):
        if (n % i == 0 or n % (i + 2) == 0):
            return False
        i = i + 6

    return True


def primes(n, c):
    """To create signatures we need hash functions (see next task). To create
    hash functions,we need prime numbers.

    Task 2: Write a script that returns the list of n consecutive prime numbers
    greater or equal to c. A simple way to test if an integer x is prime is to
    check that x is not a multiple of any integer lower or equal than sqrt(x).

    Keyword arguments:
    n -- integer representing the number of consecutive prime numbers
    c -- minimum prime number value
    """
    primes = []
    while n != 0:
        if isPrime(c):
            primes.append(c)
            n -= 1
        c += 1
    return primes


def hash_plants(s, m, p, x):
    """We will generate hash functions of the form h(x) = (ax+b) % p, where a
    and b are random numbers and p is a prime number.

    Task 3: Write a function that takes a pair of integers (m, p) and returns
    a hash function h(x)=(ax+b)%p where a and b are random integers chosen
    uniformly between 1 and m, using Python's random.randint. Write a script
    that:
        1. initializes the random seed from <seed>,
        2. generates a hash function h from <m> and <p>,
        3. returns the value of h(x).

    Keyword arguments:
    s -- value to initialize random seed from
    m -- maximum value of random integers
    p -- prime number
    x -- value to be hashed
    """
    random.seed(s)
    a = random.randint(1, m)
    b = random.randint(1, m)

    return (a * x + b) % p


def hash_list(s, m, n, i, x):
    """We will generate "good" hash functions using the generator in 3 and
    the prime numbers in 2.

    Task 4: Write a script that:
        1) creates a list of <n> hash functions where the ith hash function is
           obtained using the generator in 3, defining <p> as the ith prime
           number larger than <m> (<p> being obtained as in 1),
        2) prints the value of h_i(x), where h_i is the ith hash function in
           the list (starting at 0). The random seed must be initialized from
           <seed>.

    Keyword arguments:
    s -- seed to intialize random number generator
    m -- max value of hash random integers
    n -- number of hash functions to generate
    i -- index of hash function to use
    x -- value to hash
    """
    def hash_plants2(m, p, x):
        a = random.randint(1, m)
        b = random.randint(1, m)

        return (a * x + b) % p

    random.seed(s)
    hashes = []
    # get list of primes
    primes_list = primes(n, m)
    for p in primes_list:
        hashes.append(hash_plants2(m, p, x))

    return hashes[i]


# helper function for hash_list2: return 1 h(x)
def hash_plants2(m, p):
    a = random.randint(1, m)
    b = random.randint(1, m)

    return lambda x: (a * x + b) % p


# return list of hash functions
# takes in a list of integers (x)
def hash_list2(m, n):
    hashes = []
    # get list of primes
    primes_list = primes(n, m)

    for p in primes_list:
        hashes.append(hash_plants2(m, p))
    return hashes


def signatures(datafile, seed, n, state):
    """We will now compute the min-hash signature matrix of the states.

    Task 5: Write a function that takes build a signature of size n for a
            given state.

    1. Create the RDD of state dictionaries as in data_preparation.
    2. Generate `n` hash functions as done before. Use the number of line in
       datafile for the value of m.
    3. Sort the plant dictionary by key (alphabetical order) such that the
       ordering corresponds to a row index (starting at 0).
       Note: the plant dictionary, by default, contains the state name.
       Disregard this key-value pair when assigning indices to the plants.
    4. Build the signature array of size `n` where signature[i] is the minimum
       value of the i-th hash function applied to the index of every plant that
       appears in the given state.


    Apply this function to the RDD of dictionary states to create a signature
    "matrix", in fact an RDD containing state signatures represented as
    dictionaries. Write a script that returns the string output of the RDD
    element corresponding to state '' using function pretty_print_dict
    (provided in answers).

    The random seed used to generate the hash function must be initialized from
    <seed>, as previously.

    ***Note: Dask may be used instead of Spark.

    Keyword arguments:
    datafile -- the input filename
    seed -- seed to initialize random int generator
    n -- number of hash functions to generate
    state -- state abbreviation
    """
    spark = init_spark()
    plants_df = spark.read.text(datafile)
    m = plants_df.count()
    state_dict = plants_df.rdd.map(lambda data: data["value"].split(",")).map(list)
    state_dict = state_dict.map(lambda data: (
    data[0], dict([(i, 1) if i in data[1:] else (i, 0) for i in all_states]))).sortByKey().zipWithIndex()

    state_row = state_dict.map(lambda tup: (tup[0][1][state], tup[1]))
    # only keep those with 1
    state_row = state_row.filter(lambda x: x[0] == 1).map(lambda y: y[1]).collect()

    random.seed(seed)
    primes_list = primes(n, m)
    def hash_plant(m, p):
        a = random.randint(1, m)
        b = random.randint(1, m)
        return lambda x: (a * x + b) % p
    # get hash functions
    hash_list = [hash_plant(m, p) for p in primes_list]

    def min_hash(row):
      sigcol = []
      for i in range(len(hash_list)):
        min = float('inf')
        for k in range(len(row)):
          temp = hash_list[i](row[k])
          if temp < min:
            min = temp
        sigcol.append(min)
      return sigcol

    hashed_rows = min_hash(state_row)
    answer = dict(list(enumerate(hashed_rows)))
    return answer


def hash_band(datafile, seed, state, n, b, n_r):
    """We will now hash the signature matrix in bands. All signature vectors,
    that is, state signatures contained in the RDD computed in the previous
    question, can be hashed independently. Here we compute the hash of a band
    of a signature vector.

    Task: Write a script that, given the signature dictionary of state <state>
    computed from <n> hash functions (as defined in the previous task),
    a particular band <b> and a number of rows <n_r>:

    1. Generate the signature dictionary for <state>.
    2. Select the sub-dictionary of the signature with indexes between
       [b*n_r, (b+1)*n_r[.
    3. Turn this sub-dictionary into a string.
    4. Hash the string using the hash built-in function of python.

    The random seed must be initialized from <seed>, as previously.

    Keyword arguments:
    datafile --  the input filename
    seed -- seed to initialize random int generator
    state -- state to filter by
    n -- number of hash functions to generate
    b -- the band index
    n_r -- the number of rows
    """
    spark = init_spark()
    plants_df = spark.read.text(datafile)
    m = plants_df.count()
    state_dict = plants_df.rdd.map(lambda data: data["value"].split(",")).map(list)
    state_dict = state_dict.map(lambda data: (
    data[0], dict([(i, 1) if i in data[1:] else (i, 0) for i in all_states]))).sortByKey().zipWithIndex()

    state_row = state_dict.map(lambda tup: (tup[0][1][state], tup[1]))
    # only keep those with 1
    state_row = state_row.filter(lambda x: x[0] == 1).map(lambda y: y[1]).collect()

    random.seed(seed)
    primes_list = primes(n, m)
    def hash_plant(m, p):
        a = random.randint(1, m)
        b = random.randint(1, m)
        return lambda x: (a * x + b) % p
    # get hash functions
    hash_list = [hash_plant(m, p) for p in primes_list]
    all_columns = [[hfunc(x) for x in state_row] for hfunc in hash_list]
    answer = dict()
    for index, item in enumerate(all_columns):
        answer[index] = (min(item))
    result = {i: answer[i] for i in list(filter(lambda x: x in range(b*n_r, (b+1)*n_r), answer))}
    return hash(str(result))


def hash_bands(data_file, seed, n_b, n_r):
    """We will now hash the complete signature matrix

    Task: Write a script that, given an RDD of state signature dictionaries
    constructed from n=<n_b>*<n_r> hash functions (as in 5), a number of bands
    <n_b> and a number of rows <n_r>:

    1. maps each RDD element (using flatMap) to a list of ((b, hash),
       state_name) tuples where hash is the hash of the signature vector of
       state state_name in band b as defined in 6. Note: it is not a triple, it
       is a pair.
    2. groups the resulting RDD by key: states that hash to the same bucket for
       band b will appear together.
    3. *Return* a dictionary where the key is the band b, and the value is a list
       of buckets with 2 or more elements (note: sorting the elements in the list may be
       necessary).

    That's it, you have printed the similar items, in O(n)!

    Keyword arguments:
    datafile -- the input filename
    seed -- the seed to initialize the random int generator
    n_b -- the number of bands
    n_r -- the number of rows in a given band
    """
    spark = init_spark()
    plants_df = spark.read.text(data_file)
    m = plants_df.count()
    n = n_b * n_r
    state_dict = plants_df.rdd.map(lambda data: data["value"].split(",")).map(list)
    state_dict = state_dict.map(lambda data: (
    data[0], dict([(i, 1) if i in data[1:] else (i, 0) for i in all_states]))).sortByKey().zipWithIndex()

    random.seed(seed)
    primes_list = primes(n, m)
    def hash_plant(m, p):
        a = random.randint(1, m)
        b = random.randint(1, m)
        return lambda x: (a * x + b) % p
    # get hash functions
    hash_list = [hash_plant(m, p) for p in primes_list]

    state_row = state_dict.flatMap(lambda tup: [(state, tup[0][1][state], tup[1]) for state in all_states])
    # only keep those with 1
    state_row = state_row.filter(lambda x: x[1] == 1).map(lambda y: (y[0], y[2])).groupByKey().mapValues(list)
    state_row = state_row.map(lambda x: (x[0], [[hfunc(index) for index in x[1]] for hfunc in hash_list]))
    sigs = state_row.map(lambda x: (x[0], {i: min(vals) for i, vals in enumerate(x[1])}))
    bands = sigs.flatMap(lambda x: [((b, hash(str({i: x[1][i] for i in list(filter(lambda x: x in range(b * n_r, (b + 1) * n_r), x[1]))}))), x[0]) for b in range(n_b)]).groupByKey().mapValues(list)
    bands = bands.filter(lambda x: len(x[1]) >= 2).sortByKey()
    answer = bands.map(lambda x: (x[0][0], x[1])).groupByKey().mapValues(list)
    answer = dict(answer.collect())

    for key in answer:
        for bucket in answer[key]:
            bucket.sort()
    return answer


def get_b_and_r(n, s):
    """The script written for the previous task takes <n_b> and <n_r> as
    parameters while a similarity threshold <s> would be more useful.

    Task: Write a script that prints the number of bands <b> and rows <r> to be
    used with a number <n> of hash functions to find the similar items for a
    given similarity threshold <s>. Your script should also print <n_actual>
    and <s_actual>, the actual values of <n> and <s> that will be used, which
    may differ from <n> and <s> due to rounding issues. Printing format is
    found in tests/test-get-b-and-r.txt

    Use the following relations:

     - r=n/b
     - s=(1/b)^(1/r)

    Hint: Johann Heinrich Lambert (1728-1777) was a Swiss mathematician

    Keywords arguments:
    n -- the number of hash functions
    s -- the similarity threshold
    """
    raise Exception("Not implemented yet")
