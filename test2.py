import time, signal

import pyparade

p = None

def signal_handler(signal, frame):
	global p
	if p:
		p.stop()

if __name__ == '__main__':
	signal.signal(signal.SIGINT, signal_handler) #abort on CTRL-C

d = pyparade.Dataset(range(0,100000))

def f(a):
	#print(str(a) + "->" + str(a+1))
	#time.sleep(0.001)
	return ((a + 1) % 10, a+1)

def g(a):
	k,values = a
	return (k, sum(values)/len(values))

result = d.map(f).group_by_key().map(g)
p = pyparade.ParallelProcess(result)
p.run()
p.collect()