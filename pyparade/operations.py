import threading, multiprocessing, Queue, time

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

	def stop(self):
		if self.running.is_set():
			self._stop_requested.set()


class Operation(Source):
	def __init__(self, source, num_workers=multiprocessing.cpu_count(), initializer = None):
		super(Operation, self).__init__()
		self.source = source
		self.inbuffer = source._get_buffer()
		self.outbuffer = Queue.Queue(100)
		self.processed = 0
		self.time_started = None
		self.time_finished = None

	def __call__(self):
		self._thread = threading.Thread(target=self.run, name=str(self))
		self._thread.start()
		while not self._stop_requested.is_set() and not (self.outbuffer.empty() and self.finished.is_set()):
			try:
				yield self.outbuffer.get(True, timeout=1)
			except Exception, e:
				pass
		if self._stop_requested.is_set():
			self.time_finished = time.time()
			self.finished.set()
			self.running.clear()

class MapOperation(Operation):
	def __init__(self, source, map_func, num_workers=multiprocessing.cpu_count(), initializer = None):
		super(MapOperation, self).__init__(source, num_workers, initializer)
		self.map_func = map_func
		self.pool =  multiprocessing.Pool(num_workers, maxtasksperchild = 1000, initializer = initializer)

	def __str__(self):
		return "Map"

	def run(self, chunksize=10):
		self.running.set()
		self.enqueued = 0
		self.time_started = time.time()

		#map
		result = []
		for response in self.pool.imap(self.map_func, self.inbuffer.generate(), chunksize=chunksize):
			if not self._stop_requested.is_set():
				self.processed += 1
				self.outbuffer.put(response)
			else:
				self.pool.terminate()
				break

		self.time_finished = time.time()
		self.finished.set()
		self.running.clear()

