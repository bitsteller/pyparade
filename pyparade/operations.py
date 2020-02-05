from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import zip
from builtins import object
import threading, multiprocessing, queue, time, collections

from pyparade.util import ParMap
from pyparade.util.btree import BTree

class Source(object):
	def __init__(self, name="Source", output_name=None):
		super(Source, self).__init__()
		self._stop_requested = threading.Event()
		self.running = threading.Event()
		self.finished = threading.Event()
		self.name = name
		self.output_name = output_name

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
			return False

	def stop(self):
		if self.running.is_set():
			self._stop_requested.set()

	def __str__(self):
		return self.name

class Operation(Source):
	def __init__(self, source, num_workers=multiprocessing.cpu_count(), context = None, **kwargs):
		super(Operation, self).__init__(**kwargs)
		self.source = source
		self.inbuffer = source._get_buffer()
		self._outbuffer = queue.Queue(10)
		self._last_output = time.time()
		self._outbatch = []
		self.processed = 0
		self.time_started = None
		self.time_finished = None
		self.num_workers = num_workers
		self.context = context

	def __call__(self):
		self.running.set()
		self.time_started = time.time()

		self._thread = threading.Thread(target=self.run, name=str(self))
		self._thread.start()
		while self._thread.is_alive():
			if self._check_stop():
				return
			try:
				batch = self._outbuffer.get(True, timeout=1)
				for value in batch:
					yield value 
			except Exception as e:
				pass

		self._flush_output()

		while not self._outbuffer.empty():
			if self._check_stop():
				return
			try:
				batch = self._outbuffer.get(True, timeout=1)
				for value in batch:
					yield value 
			except Exception as e:
				pass

		self.time_finished = time.time()
		self.finished.set()
		self.running.clear()

	def _output(self, value):
		self._outbatch.append(value)
		if time.time() - self._last_output > 0.5:
			self._flush_output()

	def _flush_output(self):
		while self._outbuffer.full():
			if self._check_stop():
				return
			time.sleep(1)
		self._outbuffer.put(self._outbatch)
		self._outbatch = []
		self._last_output = time.time()

	def _check_stop(self):
		if self._stop_requested.is_set():
			self.time_finished = time.time()
		return super(Operation, self)._check_stop()

	def _generate_input(self):
		for value in self.inbuffer.generate():
			while self._outbuffer.full():
				if self._check_stop():
					raise BufferError("stop requested")
				time.sleep(1)
			yield value


class MapOperation(Operation):
	def __init__(self, source, map_func, num_workers = multiprocessing.cpu_count(), context = None, name = "Map", **kwargs):
		super(MapOperation, self).__init__(source, num_workers, context, name = name, **kwargs)
		self.map_func = map_func
		self.pool = None #multiprocessing.Pool(num_workers, maxtasksperchild = 1000, initializer = initializer)

	def run(self):
		self.pool = ParMap(self.map_func, num_workers = self.num_workers, context_func = self.context)
		#map
		result = []
		for response in self.pool.map(self._generate_input()):
			if self._check_stop():
				self.pool.stop()
				return

			self.processed += 1
			self._output(response)

class FlatMapOperation(MapOperation):
	"""Calls the map function for every value in the dataset and then flattens the result"""
	def __init__(self, source, map_func, num_workers=multiprocessing.cpu_count(), context = None, name = "FlatMap", **kwargs):
		super(FlatMapOperation, self).__init__(source, map_func, num_workers, context, name = name, **kwargs)

	def run(self):
		self.pool = ParMap(self.map_func, num_workers = self.num_workers, context_func = self.context)
		#map
		result = []
		for response in self.pool.map(self._generate_input()):
			if self._check_stop():
				self.pool.stop()
				return

			self.processed += 1
			#flatten result
			for r in response: 
				self._output(r)

class BatchOperation(Operation):
	def __init__(self, source, batch_size, name = "Batch", **kwargs):
		"""An operation that returns the elements of the source in batches of n elements.

		Args:
			source: The `pyparade.Dataset`to use as the source of elements
			batch_size: The number of elements in each batch 
			**kwargs: Other arguments are passed on to `pyparade.operations.Operation.__init__`
		"""
		super(BatchOperation, self).__init__(source, name = name, **kwargs)
		if batch_size >= 1:
			self.batch_size = batch_size
		else:
			raise ValueError("Illegal batch size")

	def run(self):
		#pack batches
		n = 0
		batch = []
		for element in self._generate_input():
			if self._check_stop():
				self.pool.stop()
				return

			batch.append(element)
			n = n + 1
			self.processed += 1
			
			if n >= self.batch_size: #batch full
				self._output(batch)
				n = 0
				batch = []
		self._output(batch)

		
class GroupByKeyOperation(Operation):
	"""Groups the key/value pairs and yields tuples (key, [list of values])"""
	def __init__(self, source, partly = False, num_workers=multiprocessing.cpu_count(), name = "GroupByKey", **kwargs):
		super(GroupByKeyOperation, self).__init__(source, num_workers, name = name, **kwargs)
		self.partly = partly

	def run_partly(self, chunksize):
		tree = BTree(chunksize, None, None)

		items_since_last_group = 0
		last_key = None
		for k, v in self._generate_input():
			if self._check_stop():
				return

			if k in tree:
				tree[k].append(v)
			else:
				tree[k] = [v]
			self.processed += 1
			items_since_last_group += 1

			if items_since_last_group >= (chunksize*self.num_workers) and k != last_key: #only output elements periodically and once key changes
				keyleafs = [l for l in tree.get_leafs()]
				for key, leaf in keyleafs:
					for k, v in zip(leaf.keys, leaf.values):
						self._output((k,v))
				tree = BTree(chunksize, None, None)
				items_since_last_group = 0

			last_key = k

		keyleafs = [l for l in tree.get_leafs()]
		for key, leaf in keyleafs:
			for k, v in zip(leaf.keys, leaf.values):
				self._output((k,v))

	def run(self, chunksize=10):
		if self.partly:
			return self.run_partly(chunksize)

		tree = BTree(chunksize, None, None)

		for k, v in self._generate_input():
			if self._check_stop():
				return

			if k in tree:
				tree[k].append(v)
			else:
				tree[k] = [v]
			self.processed += 1

		keyleafs = [l for l in tree.get_leafs()]
		for key, leaf in keyleafs:
			for k, v in zip(leaf.keys, leaf.values):
				self._output((k,v))

class ReduceByKeyOperation(Operation):
	"""Reduces the dataset by grouping the key/value pairs by key and applying the reduce_func to the values of each group"""
	def __init__(self, source, reduce_func, num_workers=multiprocessing.cpu_count(), name = "ReduceByKey", **kwargs):
		super(ReduceByKeyOperation, self).__init__(source, num_workers, name = name, **kwargs)
		self.reduce_func = reduce_func

	def run(self, chunksize=10):
		tree = BTree(chunksize, None, None)

		for k, v in self._generate_input():
			if self._check_stop():
				return

			if k in tree:
				tree[k] = self.reduce_func(tree[k], v)
			else:
				tree[k] = v
			self.processed += 1

		keyleafs = [l for l in tree.get_leafs()]
		for key, leaf in keyleafs:
			for k, v in zip(leaf.keys, leaf.values):
				self._output((k,v))

class FoldOperation(Operation):
	"""Folds the dataset using a combine function"""
	def __init__(self, source, zero_value, fold_func, num_workers=multiprocessing.cpu_count(), context = None, name = "Fold", **kwargs):
		super(FoldOperation, self).__init__(source, num_workers, context, name = name, **kwargs)
		self.pool = None #ParMap(self._fold_batch, num_workers = num_workers) #futures.ThreadPoolExecutor(num_workers)
		self.zero_value = zero_value
		self.fold_func = fold_func

	def _generate_input_batches(self, chunksize):
		batch = []
		for value in self._generate_input():
			batch.append(value)

			if len(batch) == chunksize:
				yield batch
				batch = []
		yield batch

	def _fold_batch(self, batch):
		result = self.zero_value
		for value in batch:
			result = self.fold_func(result, value)
		return result

	def run(self, chunksize = 10):
		self.pool = ParMap(self._fold_batch, num_workers = self.num_workers, context_func = self.context)
		result = []

		for response in self.pool.map(self._generate_input_batches(chunksize = chunksize)):
			if self._check_stop():
				self.pool.stop()
				return

			result.append(response)

			if len(result) == chunksize:
				result = [self._fold_batch(result)]

			self.processed += 1
		
		self._output(self._fold_batch(result))

