import threading, multiprocessing, Queue, time, collections

from util.btree import BTree

def partition(mapped_values):
	"""Organize the mapped values by their key.
	Returns an unsorted sequence of tuples with a key and a sequence of values.

	Args: 
		mapped_values: a list of tuples containing key, value pairs

	Returns:
		A list of tuples (key, [list of values])
	"""

	partitioned_data = collections.defaultdict(list)
	for key, value in mapped_values:
		partitioned_data[key].append(value)
	return partitioned_data.items()


class Source(object):
	def __init__(self):
		super(Source, self).__init__()
		self._stop_requested = threading.Event()
		self.running = threading.Event()
		self.finished = threading.Event()

	def get_parents(self):
		parents = [self]
		if isinstance(self.source, Source):
			parents.extend(self.source.get_parents())
		return parents

	def _check_stop(self):
		if self._stop_requested.is_set():
			self.finished.set()
			self.running.clear()
			return True
		else:
			#print(str(self.id) + "no stop")
			return False

	def stop(self):
		if self.running.is_set():
			self._stop_requested.set()

class Operation(Source):
	def __init__(self, source, num_workers=multiprocessing.cpu_count(), initializer = None):
		super(Operation, self).__init__()
		self.source = source
		self.inbuffer = source._get_buffer()
		self._outbuffer = Queue.Queue(100)
		self._last_output = time.time()
		self._outbatch = []
		self.processed = 0
		self.time_started = None
		self.time_finished = None
		self.num_workers = num_workers

	def __call__(self):
		self.running.set()
		self.time_started = time.time()

		self._thread = threading.Thread(target=self.run, name=str(self))
		self._thread.start()
		while not (self._outbuffer.empty() and not self._thread.is_alive()):
			if self._check_stop():
				return
			try:
				batch = self._outbuffer.get(True, timeout=1)
				for value in batch:
					yield value 
			except Exception, e:
				pass

		while self._thread.is_alive():
			if self._check_stop():
				return
			self._thread.join(1)

		self._flush_output()
		batch = self._outbuffer.get(True)
		for value in batch:
			yield value

		self.time_finished = time.time()
		self.finished.set()
		self.running.clear()

	def _output(self, value):
		self._outbatch.append(value)
		if time.time() - self._last_output > 1:
			self._flush_output()

	def _flush_output(self):
		self._outbuffer.put(self._outbatch)
		self._outbatch = []
		self._last_output = time.time()

	def _check_stop(self):
		if self._stop_requested.is_set():
			self.time_finished = time.time()
		return super(Operation, self)._check_stop()


class MapOperation(Operation):
	def __init__(self, source, map_func, num_workers=multiprocessing.cpu_count(), initializer = None):
		super(MapOperation, self).__init__(source, num_workers, initializer)
		self.map_func = map_func
		self.pool =  multiprocessing.Pool(num_workers, maxtasksperchild = 1000, initializer = initializer)

	def __str__(self):
		return "Map"

	def run(self, chunksize=10):
		#map
		result = []
		for response in self.pool.imap(self.map_func, self.inbuffer.generate(), chunksize=chunksize):
			if self._check_stop():
				self.pool.terminate()
				return

			self.processed += 1
			self._output(response)

class GroupByKeyOperation(Operation):
	"""docstring for GroupByKeyOperation"""
	def __init__(self, source, partly = False, num_workers=multiprocessing.cpu_count(), initializer = None):
		super(GroupByKeyOperation, self).__init__(source, num_workers, initializer)
		self.partly = partly
		self.pool =  multiprocessing.Pool(num_workers, maxtasksperchild = 1000, initializer = initializer)

	def __str__(self):
		return "GroupByKey"

	def run_partly(self, chunksize):
		#group
		result = []
		mapped = []
		for v in self.inbuffer.generate():
			if self.partly:
				mapped.append(v)
			else:
				result.append(v)

			if self._check_stop():
				return

			self.processed += 1

			if self.processed % (chunksize*self.num_workers) == 0:
				#partition
				partitioned_data = []
				if self.partly:
					partitioned_data = partition(mapped)
				else:
					partitioned_data = partition(result)

				if self._check_stop():
					return

				if self.partly:
					mapped = []
					for r in partitioned_data:
						self._output(r)
				else:
					result = partitioned_data

		#partition
		partitioned_data = []
		if self.partly:
			partitioned_data = partition(mapped)
		else:
			partitioned_data = partition(result)

		if self.partly:
			mapped = []
			for r in partitioned_data:
				self._output(r)
		else:
			result = partitioned_data
			for r in result:
				self._output(r)

	def run(self, chunksize=10):
		if self.partly:
			return self.run_partly(chunksize)

		tree = BTree(chunksize, None, None)

		for k, v in self.inbuffer.generate():
			if self._check_stop():
				return

			if k in tree:
				tree[k].append(v)
			else:
				tree[k] = [v]
			self.processed += 1

		keyleafs = [l for l in tree.get_leafs()]
		#keys, leafs = zip(*tree.get_leafs())
		for key, leaf in keyleafs:
			for k, v in zip(leaf.keys, leaf.values):
				self._output((k,v))
