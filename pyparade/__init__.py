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

def _signal_handler(signal, frame):
	"""Used internally. Handles interruptions (like pressing CTRL-C) and stops running processes.
	Args:
		signal: Signal
		frame: Stack frame"""
	global active_processes
	for proc in active_processes:
		print("Aborting " + proc.name + " (can take a minute)...")
		proc.stop()

class Dataset(operations.Source):
	"""Represents a dataset (can be used as a source, intermediate dataset or result)"""
	def __init__(self, source, length=None, name=None):
		"""Creates a new dataset
		Args:
			source: A source where the data for this dataset is obtained from. An iterable or `pyparade.operations.Source`
			length: The number of elements in the dataset. Only used, if len(source) is not available
			name: The display name for the dataset (convention: 1-25 characters starting with a big letter)"""
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
		"""Returns True if the length of this Source is available. 
		If this function returns False, calling len() on the Source will raise a RuntimeException."""
		return self._length != None

	def length_is_estimated(self):
		"""Returns True if the length of this Source is estimated and might change over time,
		for example if not all items in the Source are known yet."""
		return self._length_is_estimated

	def map(self, map_func, context = None, **kwargs):
		"""Returns a new `pyparade.Dataset` which results from applying map_func to each element in this Dataset.

		Supplying a context can be used to establish a connection to a common resource such as a database.

		Args:
			map_func: The function to be applied to each element. Has to accept an element from this Dataset as the first 
					  argument and should return a new element which is put into the output Dataset.
			context: A function that returns a context manager. It is called once for each parallel executor 
					 which executes map_func. For each call of map_func the context object is passed as the second argument.
			**kwargs: Other arguments are passed on to `pyparade.operations.MapOperation

		Example:
			>>> import pyparade
			>>> d = pyparade.Dataset([1,2,3])
			>>> d.map(lambda a: a + 1).collect() #add 1 to each element
			[2,3,4]

		"""
		op = operations.MapOperation(self, map_func, context = context, **kwargs)
		return Dataset(op)

	def flat_map(self, map_func, context = None, **kwargs):
		"""Returns a new `pyparade.Dataset` which results from applying map_func to each element in 
		this Dataset and combining the returned lists in a flat list.

		Supplying a context can be used to establish a connection to a common resource such as a database.

		Args:
			map_func: The function to be applied to each element. Has to accept an element from this Dataset as the first 
					  argument and should return an iterable containing elements which are put into the output Dataset.
			context: A function that returns a context manager. It is called once for each parallel executor 
					 which executes map_func. For each call of map_func the context object is passed as the second argument.
			**kwargs: Other arguments are passed on to `pyparade.operations.FlatMapOperation`

		Example:
			>>> import pyparade
			>>> d = pyparade.Dataset(["This is a test", "a b c"])
			>>> d.map(str.split).collect() #split by space (map)
			[["This", "is", "a", "test"], ["a", "b", "c"]]
			>>> d.flat_map(str.split).collect() #split by space and flat map
			["This", "is", "a", "test", "a", "b", "c"]
		"""
		op = operations.FlatMapOperation(self, map_func, context = context, **kwargs)
		return Dataset(op)

	def batch(self, batch_size=1, **kwargs):
		"""Returns a new `pyparade.Dataset` containing the elements of this dataset in batches (lists of equal length)

		Args:
			batch_size: The number of elements to put in each batch
			**kwargs: Other arguments are passed on to `pyparade.operations.BatchOperation

		Example:
			>>> import pyparade
			>>> d = pyparade.Dataset(1,2,3,4,5)
			>>> d.batch(2).collect()
			[[1,2],[3,4],[5]]

		"""
		op = operations.BatchOperation(self, batch_size=batch_size, **kwargs)
		return Dataset(op)

	def group_by_key(self, partly = False, **kwargs):
		"""Returns a new `pyparade.Dataset` which results from grouping (key,value) tuples by their key into tuples (key, [values]).

		Args:
			partly: If True, partial groups can be returned. This allows streaming processing with output 
					starting before all elements in the dataset have been processed.
			**kwargs: Other arguments are passed on to `pyparade.operations.GroupByKeyOperation

		Example:
			>>> import pyparade
			>>> d = pyparade.Dataset([("a", 1), ("b", 1), ("a",2)])
			>>> d.group_by_key().collect()
			[("a", [1,2]), ("b", [1])]

		"""
		op = operations.GroupByKeyOperation(self, partly = partly, **kwargs)
		return Dataset(op)

	def reduce_by_key(self, reduce_func, **kwargs):
		"""Returns a new `pyparade.Dataset` which results from grouping (key,value) tuples by their key into tuples (key, [values]) 
		and applying reduce_func to each (key,[values]) tuple.

		Args:
			reduce_func: The function to be applied to each (key,[values]) tuple. Has to accept a (key,[values]) tuple as the first 
					  argument and should return a new element which is put into the output Dataset.
			**kwargs: Other arguments are passed on to `pyparade.operations.ReduceByKeyOperation`
		"""
		op = operations.ReduceByKeyOperation(self, reduce_func, **kwargs)
		return Dataset(op)

	def fold(self, zero_value, fold_func, context = None, **kwargs):
		"""Returns a new `pyparade.Dataset` containing one element which results by repeatedly applying the fold function.

		Supplying a context can be used to establish a connection to a common resource such as a database.

		Args:
			zero_value: An intial value that represents zero.
			fold_func: The function to be used to fold two values. Must accept two values as arguments.
					   Must return a new value that is again accepted as an argument to fold_func.
			context: A function that returns a context manager. It is called once for each parallel executor 
					 which executes map_func. For each call of map_func the context object is passed as the second argument.
			**kwargs: Other arguments are passed on to `pyparade.operations.FoldOperation`

		Example:
			>>> import pyparade, operator
			>>> d = pyparade.Dataset([1,2,3])
			>>> d.fold(0, operator.add).collect() #sum elements
			[6]
		"""
		op = operations.FoldOperation(self, zero_value, fold_func, context = context, **kwargs)
		return Dataset(op)

	def start_process(self, name="Parallel Process", num_workers=multiprocessing.cpu_count(), **kwargs):
		"""Starts and returns a `pyparade.ParallelProcess` to collect elements in this dataset. 
		Normally called indirectly using `pyparade.Dataset.collect`.

		Args:
			name: Name of the parallel process
			num_workers: Number of parallel workers to use (default: number of available CPUs)  
			kwargs: remaining keyword arguments are passed on to ParallelProcess"""
		proc = ParallelProcess(self, name, **kwargs)
		proc.run(num_workers)
		return proc

	def _stop_process(self, process, old_handler):
		"""Internal method to stop a parellel process.

		Args:
			name: Name of the parallel process
			num_workers: Number of parallel workers to use (default: number of available CPUs)  """
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
		"""Returns a list of all elements in this dataset. 
		Starts a `ParallelProcess` in order to collect the data.

		Args:
			**args: All arguments are passed on to `pyparade.Dataset.start_process` """
		global active_processes

		old_handler = None
		proc = None
		if not self.running.is_set(): #no process running yet, start process
			proc = self.start_process(**args)
			active_processes.append(proc)
			old_handler = signal.getsignal(signal.SIGINT)
			signal.signal(signal.SIGINT, _signal_handler) #abort on CTRL-C

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

class ParallelProcess(object):
	"""A parallel process that collects data in a `pyparade.Dataset`"""
	def __init__(self, dataset, name="Parallel process", status=True, status_interval=15):
		"""Creates a new parallel process
		Args:
			dataset: The `pyparade.Dataset` which the process should collect
			name: The display name for the process
			print_status: Display status information during processing?
			print_status_interval: Update interval of status information in seconds
		"""

		self.dataset = dataset
		self.result = []
		self.name = name
		self.status = status
		self.status_interval = status_interval

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

		if self.status:
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
				if t >= self.status_interval:
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

class Buffer(object):
	"""A thread-safe buffer used to read buffered from a `pyparade.operations.Source`"""
	def __init__(self, source, size):
		"""Creates a new Buffer.

		Args:
			source: The `pyparade.operations.Source` which the buffer reads from
			size: The size of the buffer (number of elements to keep in the buffer) """

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
		"""Returns True if the buffer is full, that is the number of elements in the buffer is equal to the buffer size."""
		return self.queue.full()

	def put(self, values):
		"""Puts a batch of elements into the buffer

		Args:
			values: An iterable containing the elements to but into the buffer. Must support calling len(values)."""

		self.queue.put(values, True)
		with self._length_lock:
			self._length += len(values)

	def generate(self):		
		"""A generator yielding elements from the buffer. Runs until the underlying `pyparade.operations.Source` is finished."""

		while not(self.queue.empty() and self.source.finished.is_set()):
			try:
				values = self.queue.get(True, timeout=1)
				for value in values:
					yield value
					with self._length_lock:
						self._length -= 1
			except Exception as e:
				pass
