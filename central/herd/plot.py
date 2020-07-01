#import matplotlib.pyplot as plt
import numpy as np
#from matplotlib.backends.backend_pdf import PdfPages
import operator
import sys

all_latency = []
nr = 0

#fname = "dump_latency_numbers.txt"
fname = sys.argv[1]
f = open(fname, "r")
for line in f:
    if len(line.strip()) != 0:
        data = line.split()

        latency = data[2]
        latency = float(latency)

	# If we are counting ptr chaings,
	# we need to include non-zero ones
        if latency >= 0:
            all_latency.append(latency)
            nr = nr + 1

if nr == 0:
    print("no useful data")
    sys.exit()

all_latency = sorted(all_latency)

total_avg = []
total_90p = []
total_95p = []
total_99p = []
min_lat = []
max_lat = []
median_lat = []

total_avg.append(np.average(all_latency))
total_90p.append(np.percentile(all_latency, 90))
total_95p.append(np.percentile(all_latency, 95))
total_99p.append(np.percentile(all_latency, 99))
median_lat.append(np.median(all_latency))
max_lat.append(np.max(all_latency))

print("Min", all_latency[0])
print("Median", median_lat)
print("Avg", total_avg)
print("90P", total_90p)
print("95P", total_95p)
print("99P", total_99p)
print("Max", max_lat)
print("Total NR:", nr)
print("Sum:", np.sum(all_latency))

#plt.show()
#pp = PdfPages('fig_lru.pdf')
#pp.savefig(bbox_inches = "tight")
#pp.close()
