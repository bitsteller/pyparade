# coding=utf-8
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import object
import queue, threading, time, sys, datetime, multiprocessing, signal, threading

from . import operations

TERMINAL_WIDTH = 80

active_processes = []

def signal_handler(signal, frame):
	global active_processes
	for proc in active_processes:
		print("Aborting " + proc.name + " (can take a minute)...")
		proc.stop()

class Dataset(operations.Source):
	def __init__(self, source, length=None, name=None):
		super(Dataset, self).__init__(name)

		self.source = source
		self._length = length

		try:
			self._length = len(source)
		except Exception as e:
			pass
		if self._length != None:
			self._length_is_estimated = False
		else:
			self._length_is_estimated = True

		if self.name == None:
			if isinstance(self.source, operations.Source) and self.source.output_name != None: #get name from operation output
				self.name = self.source.output_name
			else:
				self.name = "Dataset" 

		self._buffers = []
		self.finished = threading.Event()

	def __len__(self):
		if self._length != None:
			return self._length
		else:
			raise RuntimeError("Length is not available")

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

			if time.time() - last_insert > 0.5:
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

	def map(self, map_func, context = None, **kwargs):
		op = operations.MapOperation(self, map_func, context = context, **kwargs)
		return Dataset(op)

	def flat_map(self, map_func, context = None, **kwargs):
		op = operations.FlatMapOperation(self, map_func, context = context, **kwargs)
		return Dataset(op)

	def group_by_key(self, partly = False, **kwargs):
		op = operations.GroupByKeyOperation(self, partly = partly, **kwargs)
		return Dataset(op)

	def reduce_by_key(self, reduce_func, **kwargs):
		op = operations.ReduceByKeyOperation(self, reduce_func, **kwargs)
		return Dataset(op)

	def fold(self, zero_value, fold_func, context = None, **kwargs):
		op = operations.FoldOperation(self, zero_value, fold_func, context = context, **kwargs)
		return Dataset(op)

	def start_process(self, name="Parallel Process", num_workers=multiprocessing.cpu_count()):
		proc = ParallelProcess(self, name)
		proc.run(num_workers)
		return proc

	def _stop_process(self, process, old_handler):
		global active_processes

		if old_handler != None:
			signal.signal(signal.SIGINT, old_handler)
		else:
			signal.signal(signal.SIGINT, signal.SIG_DFL)

		if process in active_processes:
			active_processes.remove(process)

		if threading.active_count() > 1:
			time.sleep(2)

		while threading.active_count() > 1:
			print("Hanging threads:")
			for t in threading.enumerate():
				if t.isAlive() and not(t == threading.current_thread()):
					print(t.name)
			time.sleep(5)
		raise RuntimeError("Process was stopped")

	def collect(self, **args):
		global active_processes

		old_handler = None
		proc = None
		if not self.running.is_set(): #no process running yet, start process
			proc = self.start_process(**args)
			active_processes.append(proc)
			old_handler = signal.getsignal(signal.SIGINT)
			signal.signal(signal.SIGINT, signal_handler) #abort on CTRL-C

		if self._stop_requested.is_set():
			self._stop_process(proc, old_handler)

		result = []
		for val in self._get_buffer().generate():
			if self._stop_requested.is_set():
				self._stop_process(proc, old_handler)

			result.append(val)

		if self._stop_requested.is_set():
			self._stop_process(proc, old_handler)

		if old_handler != None:
			signal.signal(signal.SIGINT, old_handler)
		else:
			signal.signal(signal.SIGINT, signal.SIG_DFL)
		
		if proc in active_processes:
			active_processes.remove(proc)

		return result

class Buffer(object):
	def __init__(self, source, size):
		super(Buffer, self).__init__()
		self.source = source
		self.size = size
		self.queue = queue.Queue(size)
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
					
			except Exception as e:
				pass

class ParallelProcess(object):
	def __init__(self, dataset, name="Parallel process"):
		self.dataset = dataset
		self.result = []
		self.name = name

	def run(self, num_workers = multiprocessing.cpu_count()):
		#Build process tree
		chain = self.dataset.get_parents()
		chain.reverse()
		self.chain = chain

		#set number of workers
		for operation in [block for block in chain if isinstance(block, operations.Operation)]:
			operation.num_workers = num_workers

		threads = []
		for dataset in [block for block in chain if isinstance(block,Dataset)]:
			t = threading.Thread(target = dataset._fill_buffers, name="Buffer")
			t.start()
			threads.append(t)

		ts = threading.Thread(target = self.print_status)
		ts.start()

	def stop(self):
		[s.stop() for s in self.chain]

	def clear_screen(self):
		"""Clear screen, return cursor to top left"""
		sys.stdout.write('\033[2J')
		sys.stdout.write('\033[H')
		sys.stdout.flush()

	def print_status(self):
		started = time.time()

		self.clear_screen()
		print(self.get_status())
		t = 0
		while not len([s for s in self.chain if s.finished.is_set()]) == len(self.chain):
			try:
				time.sleep(1)
				t += 1
				if t >= 10:
					self.clear_screen()
					print(self.get_status())
					t = 0
			except Exception as e:
				print(e)
				time.sleep(60)
		self.clear_screen()
		print(self.get_status())

		ended = time.time()
		print("Computation took " + str(ended-started) + "s.")

	def get_status(self):
		txt = util.shorten(str(self), TERMINAL_WIDTH) + "\n"
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

		title = "" 
		if len(op.inbuffer) > 0:
			title += " " + "(buffer: " + str(len(op.inbuffer)) + ")"
		title = util.shorten(str(op.source), TERMINAL_WIDTH - len(title) - len(status)) + title
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

		title = util.shorten(str(op), (TERMINAL_WIDTH - len(str(op)) - len(status) - 2))
		space = " "*(TERMINAL_WIDTH - len(str(title)) - len(status) - 1)
		return " " + title + space + status

	def get_result_status(self):
		status = ""
		if self.dataset.has_length():
			status = str(len(self.dataset))

		title = " (result)"
		title = util.shorten(str(self.dataset), TERMINAL_WIDTH - len(title) - len(status)) + title
		space = " "*(TERMINAL_WIDTH - len(title) - len(status))
		return title + space + status

	def __str__(self):
		return self.name
