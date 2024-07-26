import time
import tracemalloc
import csv
from algos.cminemr import cminemr

def start_benchmark():
    tracemalloc.start()
    start_time = time.time()
    return start_time

def end_benchmark(start_time):
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    end_time = time.time()
    run_time = end_time - start_time
    return run_time, peak

def load_transactions(file_path):
    list_of_lists = []
    with open(file_path, 'r', newline='') as csvfile:
        csv_reader = csv.reader(csvfile)
        i = 1
        for row in csv_reader:
            list_of_lists.append((i, row))
            i += 1
    return list_of_lists

def test_cminemr():
    datasets = {
        'mushroom': {
            'path': './datasets/mushroom.csv',
            'default': {'minRF': 0.04, 'minCS': 0.4, 'maxOR': 0.35,'NM':16},
            'variations': {
                'minRF': (0.04, [0.04, 0.2, 0.02]),
                'minCS': (0.4, [0.5, 1.0, 0.1]),
                'maxOR': (0.35, [0.05, 0.45, 0.01]),
                'NM': (16,[4,24,4])
            }
        },
        'BMS-POS': {
            'path': './datasets/BMS-POS.csv',
            'default': {'minRF': 0.04, 'minCS': 0.5, 'maxOR': 0.4,'NM':16},
            'variations': {
                'minRF': (0.04, [0.03, 0.1, 0.01]),
                'minCS': (0.5, [0.5, 1.0, 0.1]),
                'maxOR': (0.4, [0.05, 0.45, 0.01]),
                'NM': (16,[4,24,4])
            }
        },
        'Synthetic': {
            'path': './datasets/T10I4D100K.csv',
            'default': {'minRF': 0.04, 'minCS': 0.3, 'maxOR': 0.6,'NM':16},
            'variations': {
                'minRF': (0.04, [0.037, 0.05, 0.01]),
                'minCS': (0.3, [0.5, 1.0, 0.1]),
                'maxOR': (0.6, [0.05, 0.45, 0.01]),
                'NM': (16,[4,24,4])
            }
        }
    }

    for dataset_name, dataset_info in datasets.items():
        transactions = load_transactions(dataset_info['path'])
        default_params = dataset_info['default']
        variations = dataset_info['variations']
        print(f"Testing {dataset_name}")
        with open(f"./outputs/cminemr/{dataset_name}.txt", 'w') as f:
            for param, (default_value, variation_range) in variations.items():
                if param == 'minRF':
                    for minRF in [round(x, 2) for x in frange(*variation_range)]:
                        f.write(f"Testing minRF={minRF}\n")
                        f.flush()
                        start_time = start_benchmark()
                        _,shuffled_memory=cminemr(default_params['minCS'], default_params['maxOR'], minRF, transactions,default_params['NM'])
                        run_time, peak_memory = end_benchmark(start_time)
                        f.write(f"Run time: {run_time}s\n Peak memory: {peak_memory / 1024 / 1024}MB\n")
                        f.write(f"Shuffled memory: {shuffled_memory}\n")
                        f.flush()
                
                elif param == 'minCS':
                    for minCS in [round(x, 1) for x in frange(*variation_range)]:
                        f.write(f"Testing minCS={minCS}\n")
                        f.flush()
                        start_time = start_benchmark()
                        _,shuffled_memory=cminemr(minCS, default_params['maxOR'], default_params['minRF'], transactions,default_params['NM'])
                        run_time, peak_memory = end_benchmark(start_time)
                        f.write(f"Run time: {run_time}s\n Peak memory: {peak_memory / 1024 / 1024}MB\n")
                        f.write(f"Shuffled memory: {shuffled_memory}\n")
                        f.flush()
                elif param == 'maxOR':
                    for maxOR in [round(x, 2) for x in frange(*variation_range)]:
                        f.write(f"Testing maxOR={maxOR}\n")
                        f.flush()
                        start_time = start_benchmark()
                        _,shuffled_memory=cminemr(default_params['minCS'], maxOR, default_params['minRF'], transactions,default_params['NM'])
                        run_time, peak_memory = end_benchmark(start_time)
                        f.write(f"Run time: {run_time}s\n Peak memory: {peak_memory / 1024 / 1024}MB\n")
                        f.write(f"Shuffled memory: {shuffled_memory}\n")
                        f.flush()
                elif param == 'NM':
                    for nm in range(variation_range[0],variation_range[1]+1,variation_range[2]):
                        f.write(f"Testing NM={nm}\n")
                        f.flush()
                        start_time = start_benchmark()
                        _,shuffled_memory=cminemr(default_params['minCS'], default_params['maxOR'], default_params['minRF'], transactions,nm)
                        run_time, peak_memory = end_benchmark(start_time)
                        f.write(f"Run time: {run_time}s\n Peak memory: {peak_memory / 1024 / 1024}MB\n")
                        f.write(f"Shuffled memory: {shuffled_memory}\n")
                        f.flush()

def frange(start, stop, step):
    while start <= stop:
        yield start
        start += step

if __name__ == "__main__":
    test_cminemr()
    # start_time=start_benchmark()
    # transactions=load_transactions('./datasets/mushroom.csv')
    # print(cminemr(0.4,0.35,0.08,transactions,16))
    # runtime,peak=end_benchmark(start_time)
    # print(runtime)
    print("CMineMR Test completed")
