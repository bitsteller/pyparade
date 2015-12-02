import time, signal

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
	time.sleep(0.001)
	return a + 1

def g(a):
	#print(str(a) + "->" + str(a+1))
	time.sleep(0.01)
	return a + 1

inc = d.map(f).map(g).collect()
print(inc)