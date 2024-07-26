def calculate_cset(X: tuple, itd):
    cset = set()
    for w in X:
        cset.update(itd[w])
    return cset


def get_cs_or(X: tuple, itd):
    wr = (X[-1],)
    X_wr = X[:-1]
    cset_wr = calculate_cset(wr, itd)
    cset_Xwr = calculate_cset(X_wr, itd)
    intersection = cset_wr.intersection(cset_Xwr)
    union = cset_wr.union(cset_Xwr)
    cs = len(union)
    or_value = len(intersection) / len(cset_wr)
    return cs, or_value


def self_join(patterns, itd):
    new_patterns = []
    for i in range(len(patterns)):
        for j in range(i + 1, len(patterns)):
            if patterns[i][:-1] == patterns[j][:-1]:
                l1 = patterns[i][-1]
                l2 = patterns[j][-1]
                if len(itd[l1]) >= len(itd[l2]):
                    new_patterns.append(patterns[i] + (l2,))
                else:
                    new_patterns.append(patterns[j] + (l1,))
    return new_patterns


def cmine(MIN_CS, MAX_OR, MIN_RF, transactions):
    # Calculate C1, NO1, and L1
    MIN_CS = MIN_CS * len(transactions)
    # Initialise itd
    itd = {}
    for transaction in transactions:
        for item in transaction[1]:
            if item in itd.keys():
                itd[item].add(transaction[0])
            else:
                itd[item] = {transaction[0]}
    
    # Sort the frequency list
    itd = dict(sorted(itd.items(), key=lambda item: (-len(item[1]), item[0])))
    
    # Begin the algorithm
    C = []
    C.append([(x,) for x in itd.keys() if len(itd[x]) >= MIN_RF * len(transactions)])
    NO = [C[0]]
    L = []
    L.append([(x,) for x in itd.keys() if len(itd[x]) >= MIN_CS])
    number_of_patterns = len(L[-1])
    i = 1
    
    while C[-1] != []:
        i += 1
        C.append(self_join(NO[-1], itd))
        
        # Prune candidates and separate them into NO and L
        new_no = []
        new_l = []
        for pattern in C[-1]:
            cs, or_value = get_cs_or(pattern, itd)
            if or_value <= MAX_OR:
                new_no.append(pattern)
                if cs >= MIN_CS:
                    new_l.append(pattern)
        
        NO.append(new_no)
        L.append(new_l)
        number_of_patterns += len(L[-1])
    
    return number_of_patterns
