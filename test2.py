from __future__ import division
from builtins import range
import time, signal, random

import pyparade

d = pyparade.Dataset(list(range(0,1000000)))

def f(a):
	#print(str(a) + "->" + str(a+1))
	#time.sleep(0.001)
	for i in range(0,1000):
		random.random()

	return ((a + 1) % 10, a+1)

def g(a):
	k,values = a
	return (k, sum(values)/len(values))

result = d.map(f).group_by_key().map(g).collect()