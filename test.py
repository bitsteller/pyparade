import time

import pyparade

d = pyparade.Dataset(range(0,1000000), name="Numbers with a really extremly unnecessarly long dataset name for no reason")

def f(a):
	#print(str(a) + "->" + str(a+1))
	time.sleep(0.001)
	return a + 1

def g(a):
	#print(str(a) + "->" + str(a+1))
	time.sleep(0.01)
	return a + 1

inc = d.map(f, name="add 1", output_name="Numbers+1").map(g, name="add 1", output_name="Numbers+2").collect()
print(inc)