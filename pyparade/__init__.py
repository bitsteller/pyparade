# coding=utf-8
import Queue, threading, time, sys, datetime, multiprocessing

import operations

TERMINAL_WIDTH = 80

class Dataset(operations.Source):
	def __init__(self, source, length=None):
		super(Dataset, self).__init__()

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
		self.finished = threading.Event()

	def __len__(self):
		if self._length != None:
			return self._length
		else:
			raise RuntimeError("Length is not available")

	def __str__(self):
		return "Dataset"

	def _get_buffer(self, size = 30):
		buf = Buffer(self, size=size)
		self._buffers.append(buf)
		return buf

	def _fill_buffers(self):
		self.running.set()
		if isinstance(self.source, operations.Operation):
			values = self.source()
		else:
			values = self.source

		if self._length_is_estimated:
			self._length = 0

		batch = []
		last_insert = 0
		for value in values:
			if self._check_stop():
				return

			while len([buf for buf in self._buffers if buf.full()]) > 0:
				if self._check_stop():
					return
				time.sleep(1)
			batch.append(value)

			if self._length_is_estimated:
				self._length += 1

			if time.time() - last_insert > 1:
				[buf.put(batch) for buf in self._buffers]
				batch = []
				last_insert = time.time()

		while len([buf for buf in self._buffers if buf.full()]) > 0:
			if self._check_stop():
				return
			time.sleep(1)

		[buf.put(batch) for buf in self._buffers]

		self._length_is_estimated = False
		self.finished.set()
		self.running.clear()

	def has_length(self):
		return self._length != None

	def length_is_estimated(self):
		return self._length_is_estimated

	def map(self, map_func):
		op = operations.MapOperation(self, map_func)
		return Dataset(op)

	def flat_map(self, map_func):
		op = operations.FlatMapOperation(self, map_func)
		return Dataset(op)

	def group_by_key(self, partly = False):
		op = operations.GroupByKeyOperation(self, partly = partly)
		return Dataset(op)

	def fold(self, zero_value, fold_func):
		op = operations.FoldOperation(self, zero_value, fold_func)
		return Dataset(op)

class Buffer(object):
	def __init__(self, source, size):
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

	def put(self, values):
		self.queue.put(values, True)
		with self._length_lock:
			self._length += len(values)

	def generate(self):
		while not(self.queue.empty() and self.source.finished.is_set()):
			try:
				values = self.queue.get(True, timeout=1)
				for value in values:
					yield value
					with self._length_lock:
						self._length -= 1
					
			except Exception, e:
				pass

class ParallelProcess(object):
	def __init__(self, dataset, title="Parallel process"):
		self.dataset = dataset
		self.result = []
		self.buffer = self.dataset._get_buffer(size=None)
		self.title = title

	def run(self, num_workers = multiprocessing.cpu_count()):
		#Build process tree
		chain = self.dataset.get_parents()
		chain.reverse()
		self.chain = chain

		#set number of workers
		for operation in [block for block in chain if isinstance(block,operations.Operation)]:
			operation.num_workers = num_workers

		started = time.time()
		threads = []
		for dataset in [block for block in chain if isinstance(block,Dataset)]:
			t = threading.Thread(target = dataset._fill_buffers, name="Buffer")
			t.start()
			threads.append(t)

		ts = threading.Thread(target = self.print_status)
		ts.start()
		while ts.is_alive():
			ts.join(1)

		ended = time.time()
		print("Computation took " + str(ended-started) + "s.")

	def stop(self):
		[s.stop() for s in self.chain]

	def clear_screen(self):
		"""Clear screen, return cursor to top left"""
		sys.stdout.write('\033[2J')
		sys.stdout.write('\033[H')
		sys.stdout.flush()

	def print_status(self):
		self.clear_screen()
		print(self.get_status())
		while not len([s for s in self.chain if s.finished.is_set()]) == len(self.chain):
			try:
				time.sleep(10)
				self.clear_screen()
				print(self.get_status())
			except Exception, e:
				print(e)
				time.sleep(60)
		self.clear_screen()
		print(self.get_status())

	def get_status(self):
		txt = self.title + "\n"
		txt += ("=" * TERMINAL_WIDTH) + "\n"
		txt += "\n".join([self.get_buffer_status(op) + "\n" + self.get_operation_status(op) for op in self.chain if isinstance(op, operations.Operation)])
		txt += "\n" + self.get_result_status()
		return txt

	def get_buffer_status(self, op):
		status = ""

		if not op.source.length_is_estimated():
			status += str(len(op.source))
		elif not op.source.running.is_set():
			status += "stopped"

		title = "Dataset (buffer: " + str(len(op.inbuffer)) + ")"
		space = " "*(TERMINAL_WIDTH - len(title) - len(status))
		return title + space + status

	def get_operation_status(self, op):
		status = ""

		if op.source.has_length():
			if not op.source.length_is_estimated() and len(op.source) > 0 and op.processed > 0:
				if op.processed == len(op.source):
					status += "done"
				elif op.running.is_set():
					est = datetime.datetime.now() + datetime.timedelta(seconds = (time.time()-op.time_started)/op.processed*(len(op.source)-op.processed))
					status += '{0:%}'.format(float(op.processed)/len(op.source)) + "  ETA " + est.strftime("%Y-%m-%d %H:%M") + " "
					status += str(op.processed) + "/" + str(len(op.source))
				else:
					status += "stopped"
			else:
				status += str(op.processed) + "/" + str(len(op.source))
		else:
			if not op.running.is_set():
				status += "stopped"	

		space = " "*(TERMINAL_WIDTH - len(str(op)) - len(status) - 1)
		return " " + str(op) + space + status

	def get_result_status(self):
		status = ""
		if self.dataset.has_length():
			status = str(len(self.dataset))

		title = "Dataset (result)"
		space = " "*(TERMINAL_WIDTH - len(title) - len(status))
		return title + space + status		

	def collect(self):
		#if self.dataset._stop_requested.is_set():
		#	raise RuntimeError("Process was stopped")
		#if not self.dataset.finished.is_set():
		#	raise RuntimeError("Process is not finished")

		result = []
		for val in self.buffer.generate():
			if self.dataset._stop_requested.is_set():
				raise RuntimeError("Process was stopped")
			result.append(val)
		print(result)
		return result
			#time.sleep(1)
		#return self.buffer.generate()
