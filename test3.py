import time, signal, random, operator

import pyparade

p = None

def signal_handler(signal, frame):
	global p
	if p:
		p.stop()

if __name__ == '__main__':
	signal.signal(signal.SIGINT, signal_handler) #abort on CTRL-C

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

result = d.map(f).map(g).fold(0,operator.add)
p = pyparade.ParallelProcess(result)
p.run()
p.collect()