from pyspark import SparkConf, SparkContext
# MapReduce for each phase of the dcpm algorithm


def map_phase_1(record):
    """
    Map phase 1 for the dcpm algorithm.
    """
    items_dict = {}
    for item in record[1]:
        if item in items_dict.keys():
            items_dict[item].append(record[0])
        else:
            items_dict[item] = [record[0]]

    return [(key, value) for key, value in items_dict.items()]


def reduce_phase_1(list1, list2):
    """
    Reduce phase 1 for the dcpm algorithm.
    """
    list1.extend(list2)
    return list1


def map_phase_2(pattern, itd_bc, MAX_OR):
    """
    Map phase 2 for the dcpm algorithm.
    """
    lli = []
    itd = itd_bc.value
    merge = False
    cs = []
    for key in itd.keys():
        if key == pattern[0]:
            merge = True
        elif merge and key != pattern[0]:
            OR = len(set(itd[key]).intersection(
                set(itd[pattern[0]])))/len(itd[key])
            if OR <= MAX_OR:
                lli.append(key)
                cs.append(len(set(itd[key]).union(set(itd[pattern[0]]))))

    return [(pattern, (lli, cs))]

# For generating PPCs from patterns in NO


def get_cs(pattern, itd):
    CS = set()
    for item in pattern:
        CS.update(itd[item])
    return CS

# Generating new PPCs from the k-1 PPCs


def map_PPC(ppc, itd_bc, MAX_OR):
    PPC = []
    CPP, LLI, CS_list = ppc[0], ppc[1][0], ppc[1][1]
    itd = itd_bc.value
    if len(LLI) < 2:
        return PPC
    else:
        for i in range(len(LLI)-1):
            CS_CPP = get_cs(CPP+(LLI[i],), itd)
            for j in range(i+1, len(LLI)):
                CS_j = get_cs((LLI[j],), itd)
                OR = len(CS_CPP.intersection(CS_j))/len(CS_j)
                if OR <= MAX_OR:
                    PPC.append(
                        (CPP+(LLI[i],), ([LLI[j]], [len(CS_CPP.union(CS_j))])))
    return PPC


def reduce_PPC(tuple1, tuple2):
    return (tuple1[0]+tuple2[0], tuple1[1]+tuple2[1])


# For generating NO from PPC
def generate_NO(partition):
    NO = []
    for ppc in partition:
        CPP, LLI = ppc[0], ppc[1][0]
        for item in LLI:
            NO.append(CPP+(item,))
    return NO


# For generating CPs from PPC
def generate_CP(iter, MIN_CS):
    CP = []
    for ppc in iter:
        CPP, LLI, CS_list = ppc[0], ppc[1][0], ppc[1][1]
        for i in range(len(LLI)):
            item = LLI[i]
            CS = CS_list[i]
            if CS >= MIN_CS:
                CP.append(CPP+(item,))
    return CP


# Main function for the dcpm algorithm

def dcpm(MIN_CS, MAX_OR, MIN_RF, transactions, num_cores):
    # Configuration
    app_name = "DCPM"
    conf = SparkConf().setAppName(app_name).setMaster(f"local[{num_cores}]") \
                      .set("spark.default.parallelism", f"{2*num_cores}") \
                      .set("spark.executor.memory", "1400m") \
                      .set("spark.driver.memory", "4g")
    sc = SparkContext(conf=conf)
    # Initialize the variables
    number_of_patterns = 0
    NO = []
    CP = []
    MIN_CS = MIN_CS*len(transactions)
    # Phase 1
    transactions_rdd = sc.parallelize(transactions)
    map_phase_1_rdd = transactions_rdd.flatMap(map_phase_1)
    reduce_phase_1_rdd = map_phase_1_rdd.reduceByKey(reduce_phase_1)
    itd = reduce_phase_1_rdd.collectAsMap()
    # Remove infrequent items
    itd_keys = list(itd.keys())
    for key in itd_keys:
        if len(itd[key]) < MIN_RF*len(transactions):
            itd.pop(key)
    # Sort the ITD
    itd = dict(sorted(itd.items(), key=lambda item: (-len(item[1]), item[0])))
    # Generate NO1 and CP1
    NO.append([(key,) for key in itd.keys()])
    CP.append([(key,) for key in itd.keys() if len(itd[key]) >= MIN_CS])
    number_of_patterns += len(CP[0])
    # Broadcast the ITD
    itd_bc = sc.broadcast(itd)
    # Phase 2
    NO1_rdd = sc.parallelize(NO[0])
    mapped_NO1 = NO1_rdd.flatMap(lambda x: map_phase_2(x, itd_bc, MAX_OR))
    # Generate NO and CP
    NO2 = mapped_NO1.mapPartitions(generate_NO).collect()
    CP2 = mapped_NO1.mapPartitions(
        lambda iter: generate_CP(iter, MIN_CS)).collect()
    CP.append(CP2)
    number_of_patterns += len(CP2)
    NO.append(NO2)
    PPC_rdd = mapped_NO1
    # Phase 3
    while len(NO[-1]):
        PPC_new = PPC_rdd.flatMap(lambda x: map_PPC(
            x, itd_bc, MAX_OR)).reduceByKey(reduce_PPC)
        CP_new = PPC_new.mapPartitions(
            lambda iter: generate_CP(iter, MIN_CS)).collect()
        NO_new = PPC_new.mapPartitions(generate_NO).collect()
        CP.append(CP_new)
        NO.append(NO_new)
        number_of_patterns += len(CP_new)
        PPC_rdd = PPC_new

    sc.stop()
    return number_of_patterns, 0
