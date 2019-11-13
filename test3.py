from __future__ import print_function
from builtins import range
import time, signal, random, operator

import pyparade

def f(a):
	#print(str(a) + "->" + str(a+1))
	#time.sleep(0.001)
	for i in range(0,1000):
		random.random()

	return ((a + 1) % 100000, a+1)

def g(kv):
	k,v = kv
	return v

result = pyparade.Dataset(list(range(0,1000000)), name="Numbers") \
			.map(f, name="calculate", output_name="Key/Value pairs") \
		 	.map(g, name="take value", output_name="Values") \
		 	.fold(0,operator.add,name="sum", output_name="Sum").collect()

print(result)