import time, signal, random, operator

import pyparade

d = pyparade.Dataset(range(0,1000000))

def f(a):
	#print(str(a) + "->" + str(a+1))
	#time.sleep(0.001)
	for i in range(0,1000):
		random.random()

	return ((a + 1) % 100000, a+1)

def g(kv):
	k,v = kv
	return v

result = d.map(f).map(g).fold(0,operator.add).collect()