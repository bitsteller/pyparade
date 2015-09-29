import time

import pyparade

d = pyparade.Dataset(range(0,100000))

def f(a):
	#print(str(a) + "->" + str(a+1))
	time.sleep(0.1)
	return a + 1

inc = d.map(f)
inc2 = inc.map(f)
p = pyparade.ParallelProcess(inc2)
p.run()
p.collect()