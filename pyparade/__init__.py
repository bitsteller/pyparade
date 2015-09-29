# coding=utf-8
import Queue, threading, time

import operations

class Dataset(operations.Source):
	def __init__(self, source, length=None):
		self.source = source
		self._length = None
		self._buffers = []
		self._eof = threading.Event()

		try:
			self._length = len(source)
		except Exception, e:
			pass

	def __len__(self):
		return self._length

	def __str__(self):
		return "Dataset"

	def _get_buffer(self):
		buf = Buffer(self)
		self._buffers.append(buf)
		return buf

	def _fill_buffers(self):
		if isinstance(self.source, operations.Operation):
			values = self.source()
		else:
			values = self.source

		for value in values:
			while len([buf for buf in self._buffers if buf.full()]) > 0:
				time.sleep(1)
			[buf.put(value) for buf in self._buffers]
		self._eof.set()

	def map(self, function):
		op = operations.MapOperation(self, function)
		return Dataset(op)

class Buffer(object):
	def __init__(self, source, size = 1000):
		super(Buffer, self).__init__()
		self.source = source
		self.size = size
		self.queue = Queue.Queue(size)
		self._length = 0
		self._length_lock = threading.Lock()

	def __len__(self):
		with self._length_lock:
			return self._length

	def full(self):
		return self.queue.full()

	def put(self, obj):
		self.queue.put(obj, True)
		with self._length_lock:
			self._length += 1

	def generate(self):
		while not(self.queue.empty() and self.source._eof.is_set()):
			try:
				yield self.queue.get(True, timeout=1)
				with self._length_lock:
					self._length -= 1
			except Exception, e:
				print("buffer empty")
				pass

class ParallelProcess(object):
	def __init__(self, dataset, title="Parallel process"):
		self.dataset = dataset
		self.result = []
		self.buffer = self.dataset._get_buffer()
		self.title = title

	def run(self):
		#Build process tree
		chain = self.dataset.get_parents()
		chain.reverse()
		self.chain = chain

		threads = []
		for dataset in [block for block in chain if isinstance(block,Dataset)]:
			t = threading.Thread(target = dataset._fill_buffers, name="Buffer")
			t.start()
			threads.append(t)

		ts = threading.Thread(target = self.print_status)
		ts.start()

	def print_status(self):
		print("Status")
		while not self.dataset.source.finished.is_set():
			txt = self.title + "\n"
			txt += "=============================================\n"
			txt += "\n".join([self.get_buffer_status(op) + "\n" + self.get_operation_status(op) for op in self.chain if isinstance(op, operations.Operation)])
			print(txt)
			time.sleep(1)

	def get_buffer_status(self, op):
		return "Dataset (buffer: " + str(len(op.inbuffer)) + ")"

	def get_operation_status(self, op):
		return " " + str(op) + "			" + str(op.processed)

	def collect(self):
		for val in self.buffer.generate():
			print(val)
			#time.sleep(1)
		#return self.buffer.generate()
