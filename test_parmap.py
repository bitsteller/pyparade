import signal, random

from pyparade.util import ParMap

p = None

def signal_handler(signal, frame):
	global p
	if p:
		p.stop()

if __name__ == '__main__':
	signal.signal(signal.SIGINT, signal_handler) #abort on CTRL-C

def f(a):
	#print(str(a) + "->" + str(a+1))
	#time.sleep(0.001)
	for i in range(0,1000):
		random.random()

	return ((a + 1) % 100000, a+1)

def g(kv):
	k,v = kv
	return v

p = ParMap(f, num_workers = 16)
count = 0
for r in p.map(range(1000000)):
	count += 1
	print(r)
print(count)