from pyspark import SparkConf, SparkContext
"""CMineMR algorithm implementation in PySpark."""
# Self-joins for phase 2
def self_join_phase_2(patterns,flist_br):
    new_patterns=[]
    flist=flist_br.value
    for i in range(0,len(patterns)-1):
        for j in range(i+1,len(patterns)):
            if patterns[i][:-1]==patterns[j][:-1]:
                l1=patterns[i][-1]
                l2=patterns[j][-1]
                if flist[l1]>=flist[l2]:
                    new_patterns.append(patterns[i]+(l2,))
                else:
                    new_patterns.append(patterns[j]+(l1,))
    return new_patterns



# MapReduce for each phase of the CMineMR algorithm
def map_phase_1(record):
    """
    Map phase 1 for the CMineMR algorithm.
    """
    count=[]
    for item in record[1]:
        count.append(tuple([item,1]))
    return count

def reduce_phase_1(value1, value2):
    """
    Reduce phase 1 for the CMineMR algorithm.
    """
    return value1 + value2

def map_phase_2(transaction,patterns):
    """
    Map phase 2 for the CMineMR algorithm.
    """
    mapped_values=[]
    for pattern in patterns:
            intersection = set(pattern).intersection(set(transaction[1]))
            if len(intersection) >= 1:
                if pattern[-1] in intersection and len(intersection)> 1:
                           mapped_values.append((pattern,(1,1)))
                else:
                    mapped_values.append((pattern,(1,0)))
            else:
                mapped_values.append((pattern,(0,0)))
    return mapped_values

def reduce_phase_2(value1, value2):
    """Reduce Phase II for the CMineMR algorithm."""
    return tuple([value1[0]+value2[0],value1[1]+value2[1]])

def map_phase_3(pattern:list):
     key=pattern[:-1]
     return [(key,pattern[-1])]

def reduce_phase_3(a,b):
     combined=[]
     if type(a)!=list:
      combined = [a]
     else:
      combined=a
     if type(b)==list:    
      combined.extend(b)
     else:
      combined.append(b)
     return combined
     
def get_cs_and_or(data,C,trx_len,flist_broadcast):
    map_pattern=data.flatMap(lambda x: map_phase_2(x,C))
    reduce_pattern=map_pattern.reduceByKey(reduce_phase_2)
    cs_or_rdd = reduce_pattern.map(lambda item: (item[0], 
                                                 (item[1][0] / trx_len, 
                                                  item[1][1] / flist_broadcast.value[item[0][-1]])))
    

    return cs_or_rdd.collect()    


# Generate candidates
def generate_candidates(pattern,flist_br):
          candidates = []
          flist=flist_br.value
          if isinstance(pattern[1], list) and len(pattern[1]) >= 2:
            for i in range(len(pattern[1]) - 1):
                for j in range(i + 1, len(pattern[1])):
                    if flist[pattern[1][i]] > flist[pattern[1][j]]:
                        candidates.append(pattern[0] + (pattern[1][i], pattern[1][j]))
                    elif flist[pattern[1][i]] == flist[pattern[1][j]]:
                        if pattern[1][i] < pattern[1][j]:
                            candidates.append(pattern[0] + (pattern[1][i], pattern[1][j]))
                        else:
                            candidates.append(pattern[0] + (pattern[1][j], pattern[1][i]))
                    else:
                        candidates.append(pattern[0] + (pattern[1][j], pattern[1][i]))
          return candidates





# CMineMR algorithm


def cminemr(MIN_CS,MAX_OR,MIN_RF,transactions,num_cores):
    # Configuration
    app_name = "CMine MR"
    conf = SparkConf().setAppName(app_name).setMaster(f"local[{num_cores}]").set("spark.default.parallelism",f"{2*num_cores}")

    sc = SparkContext(conf=conf)
    flist={}
    trx_len=len(transactions)
    # Distribute the transactions
    data = sc.parallelize(transactions)

    # Perform Phase I of the CMineMR algorithm
    map_1=data.flatMap(map_phase_1)
    reduce_1=map_1.reduceByKey(reduce_phase_1)
    for item in reduce_1.collect():
        flist[item[0]]=item[1]
    
    # Sort the frequency list
    flist = dict(sorted(flist.items(), key=lambda item: (-item[1],item[0])))
    number_of_patterns=0
    
    NO=[]
    L=[]
    C=[]
    # Calculate NO1 and L1
    C.append([(x,) for x in flist.keys() if flist[x] >= MIN_RF*trx_len])
    NO.append([(x,) for x in flist.keys() if flist[x] >= MIN_RF*trx_len])
    L.append([(x,) for x in NO[0] if flist[x[0]] >= MIN_CS*trx_len])
    number_of_patterns+=len(L[0])
    # Phase II of the CMineMR algorithm
    flist_br=sc.broadcast(flist)
    C.append(self_join_phase_2(NO[0],flist_br))

    C2=C[1]
    C2_cs_or=get_cs_and_or(data,C2,trx_len,flist_br)
    NO.append([x[0] for x in C2_cs_or if x[1][1] <= MAX_OR])
    L.append([x[0] for x in C2_cs_or if x[1][0]>=MIN_CS and x[1][1]<=MAX_OR])
    number_of_patterns+=len(L[1])
    i=3
    while C[-1]!=[]:
         # First generate Ck
         non_overlap=sc.parallelize(NO[-1])
         candidate_map=non_overlap.flatMap(map_phase_3)
         candidate_reduce=candidate_map.reduceByKey(reduce_phase_3)
         candidates=[]
         
         

        # Use flatMap to generate candidates
         candidates = candidate_reduce.flatMap(lambda x:generate_candidates(x,flist_br)).collect()
         C.append(candidates)
         candidates_br=C[-1]
         C_cs_or=get_cs_and_or(data,candidates_br,trx_len,flist_br)
         NO.append([x[0] for x in C_cs_or if x[1][1] <= MAX_OR])
         L.append([x[0] for x in C_cs_or if x[1][0]>=MIN_CS and x[1][1]<=MAX_OR])
         number_of_patterns+=len(L[-1])
         i+=1
    
    
    sc.stop()
    return number_of_patterns,0



