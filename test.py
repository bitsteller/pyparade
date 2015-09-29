import pyparade

d = pyparade.Dataset(range(0,100000))

def f(a):
	print(str(a) + "->" + str(a+1))
	return a + 1

inc = d.map(f)
p = pyparade.ParallelProcess(inc)
p.run()
p.collect()