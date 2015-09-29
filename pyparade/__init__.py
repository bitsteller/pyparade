# coding=utf-8
import Queue, threading, time, sys, datetime

import operations

TERMINAL_WIDTH = 80

class Dataset(operations.Source):
	def __init__(self, source, length=None):
		self.source = source
		self._length = length
		try:
			self._length = len(source)
		except Exception, e:
			pass
		if self._length != None:
			self._length_is_estimated = False
		else:
			self._length_is_estimated = True

		self._buffers = []
		self._eof = threading.Event()

	def __len__(self):
		if self._length != None:
			return self._length
		else:
			raise RuntimeError("Length is not available")

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

		if self._length_is_estimated:
			self._length = 0

		for value in values:
			while len([buf for buf in self._buffers if buf.full()]) > 0:
				time.sleep(1)
			[buf.put(value) for buf in self._buffers]
			if self._length_is_estimated:
				self._length += 1
		self._length_is_estimated = False
		self._eof.set()

	def has_length(self):
		return self._length != None

	def length_is_estimated(self):
		return self._length_is_estimated

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

	def clear_screen(self):
		"""Clear screen, return cursor to top left"""
		sys.stdout.write('\033[2J')
		sys.stdout.write('\033[H')
		sys.stdout.flush()

	def print_status(self):
		while not self.dataset.source.finished.is_set():
			try:
				self.clear_screen()
				txt = self.title + "\n"
				txt += ("=" * TERMINAL_WIDTH) + "\n"
				txt += "\n".join([self.get_buffer_status(op) + "\n" + self.get_operation_status(op) for op in self.chain if isinstance(op, operations.Operation)])
				print(txt)
				time.sleep(1)
			except Exception, e:
				print(e)
				time.sleep(3)


	def get_buffer_status(self, op):
		return "Dataset (buffer: " + str(len(op.inbuffer)) + ")"

	def get_operation_status(self, op):
		status = ""
		if op.source.has_length():
			status = ""
			if not op.source.length_is_estimated() and len(op.source) > 0 and op.processed > 0:
				if op.processed == len(op.source):
					status += "done "
				else:
					est = datetime.datetime.now() + datetime.timedelta(seconds = (time.time()-op.time_started)/op.processed*(len(op.source)-op.processed))
					status += '{0:%}'.format(float(op.processed)/len(op.source)) + "  ETA " + est.strftime("%Y-%m-%d %H:%M") + " "
			status += str(op.processed) + "/" + str(len(op.source))
		else:
			status = str(op.processed)
			

		space = " "*(TERMINAL_WIDTH - len(str(op)) - len(status) - 1)
		return " " + str(op) + space + status

	def collect(self):
		result = []
		for val in self.buffer.generate():
			result.append(val)
		print(result)
		return result
			#time.sleep(1)
		#return self.buffer.generate()
