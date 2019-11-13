# coding=utf8
from __future__ import print_function
from __future__ import division
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import range
from builtins import object
import multiprocessing, time, math, traceback, queue
from multiprocessing import Process
import multiprocessing

def sstr(obj):
	""" converts any object to str, if necessary encodes unicode chars """
	try:
		return str(obj)
	except UnicodeEncodeError:
		return str(obj).encode('utf-8')

def shorten(text, max_length=140):
	min_length = 0.8*max_length
	shortstr = ""

	if len(text) <= max_length:
		return text

	words = text.split(" ")
	for word in words:
		if len(shortstr) < min_length:
			if len(shortstr) + len(word) + len(" ") <= max_length:
				shortstr += word + " "
			else:
				shortstr = text[0:max_length-2] + " "
	return shortstr[:-1] + "..."


class Event(object):
	"""An event that can have multiple handlers """
	def __init__(self):
		self.handlers = []
	
	def add(self, handler):
		"""Add a handler that is called when the event fires
		Args:
			handler: a callable that is called when the event is fired. 
			The arguments passed to the handler are the sender and additionally arguments that were passed to the fire() function"""
		self.handlers.append(handler)
		return self
	
	def remove(self, handler):
		"""Removes a handler, such that it is no no longer called when the event fires
		Args:
			handler: the handler function to remove"""
		self.handlers.remove(handler)
		return self
	
	def fire(self, sender, earg=None):
		"""Fires the event and calls all registered handlers
		Args:
			sender: the object that fired the event
			earg: Optional argument containing additional information that should be passed to all handlers"""
		for handler in self.handlers:
			handler(sender, earg)
	
	__iadd__ = add
	__isub__ = remove
	__call__ = fire

class Timer(object):
	"""Measures time from when the timer is created and until stop() is called"""
	def __init__(self, description):
		"""Creates a new timer and starts it.
		Args:
			description: a description of what is measured
		"""
		super(Timer, self).__init__()
		self.start = time.time()
		self.description = description

	def stop(self):
		"""Stops the timer. Prints the number of seconds elapsed on console"""
		self.end = time.time()
		self.seconds = self.end-self.start
		print(self.description + " took " + str(self.seconds) + "s.")

class ParMap(object):
	"""Parallel executes a map in several processes"""
	def __init__(self, map_func, num_workers=multiprocessing.cpu_count(), context_func = None):
		"""A parallel implementation of map() that uses multiple processes to divide the compuation.
		Args:
			map_func: the function to apply to each element
			context_func: an optional function that returns a context object (that could be used in a with block). The function is called once for each worker and the context object is passed to the map_func.
			num_workers: the number of worker processes to spawn (defaults to the number of CPU cores available)
		"""
		super(ParMap, self).__init__()
		self.map_func = map_func
		self.context_func = context_func
		self.num_workers = num_workers
		self.request_stop = multiprocessing.Event()
		self._chunksize = 1
		self.chunkseconds = 3.0

	@property
	def chunksize(self):
		return self._chunksize

	def stop(self):
		"""Requests all active workers connected to a running parallel mapping to stop.
		Note that stopping workers is done asynchrously and can take while, even though this function returns immediately."""

		self.request_stop.set()

	def job_is_finished(self, job, timeout = 0.0):
		if "stopped" in job:
			return True
		elif job["worker"] != None:
			return job["worker"]["connection"].poll(timeout)
		else:
			return False

	def map(self, iterable):
		"""Applies the map_func of the ParMap object to all elements in the iterable using parallel worker processes. The result is returned as a generator.
		An optimal chunksize that is submitted to the workers is calculated dynamically.
		Results are calculated on demand, meaning that you have to loop through the generator to continue processing.
		Elements in the iterable have to be pickable to be able to submit them to worker processes.
		
		Args:
			iterable: an iterable (list or generator) that map_func is applied to
		"""
		#initialize
		workers = []
		free_workers = queue.Queue()
		self.request_stop = multiprocessing.Event()
		self._chunksize = 1

		#start up workers
		for i in range(self.num_workers):
			parent_conn, child_conn = multiprocessing.Pipe()
			worker = {} 
			worker["connection"] =  parent_conn
			worker["process"] = Process(target = self._work, args=(child_conn, self.context_func))
			workers.append(worker)
			free_workers.put(worker)
			worker["process"].start()

		#process values
		jobs = {}
		jobid = 0
		batch = []
		last_processing_times = [self.chunkseconds] * 10*self.num_workers #init with chunkseconds, such that intial chunksize is 1
		last_processing_time_pos = 0

		for value in iterable:
			#wait as long as all workers are busy
			while free_workers.empty():
				minjobid = min(jobs.keys())
				jobs[minjobid]["worker"]["connection"].poll(0.1)
				for job in jobs.values():
					if (not "stopped" in job) and job["worker"]["connection"].poll():
						job.update(job["worker"]["connection"].recv())
						free_workers.put(job["worker"])
						job["worker"] = None

			#if job limit reached, wait for leftmost job to finish
			if len(jobs) >= 10*self.num_workers: #do not start jobs for more than 10*workers batches ahead to save memory
				while not (self.job_is_finished(jobs[min(jobs.keys())], timeout = 0.1)):
					pass

			#yield results while leftmost batch is ready
			while len(jobs) > 0 and (self.job_is_finished(jobs[min(jobs.keys())])): 
				minjobid = min(jobs.keys())
				if (not "stopped" in jobs[minjobid]) and jobs[minjobid]["worker"]["connection"].poll(): #FIXME: worker might have new job!!!!
					jobs[minjobid].update(jobs[minjobid]["worker"]["connection"].recv())
					free_workers.put(jobs[minjobid]["worker"])
					jobs[minjobid]["worker"] = None

				#update optimal chunksize based on 10*workers last batch processing times
				last_processing_times[last_processing_time_pos] = (jobs[minjobid]["stopped"] - jobs[minjobid]["started"])/self._chunksize
				last_processing_time_pos = (last_processing_time_pos + 1) % (10*self.num_workers) #rotate through list with last processing times

				avg_processing_time = sum(last_processing_times)/len(last_processing_times)
				desired_chunksize = int(math.ceil(self.chunkseconds/avg_processing_time)) #batch should take chunkseconds s to calculate
				self._chunksize = min(desired_chunksize, max(10,2*self._chunksize)) #double chunksize at most every time (but allow to go to 10 directly in the beginning)
				#print(self._chunksize)

				if "error" in jobs[minjobid]:
					raise jobs[minjobid]["error"]

				for r in jobs[minjobid]["results"]:
					yield r
				del jobs[minjobid]

			#start new job if batch full
			batch.append(value)

			if len(batch) >= self._chunksize:
				job = {}
				jobs[jobid] = job
				job["started"] = time.time()
				job["worker"] = free_workers.get()
				job["worker"]["connection"].send(batch)

				batch = []
				jobid += 1

		#wait while all workers busy
		while free_workers.empty():
			minjobid = min(jobs.keys())
			jobs[minjobid]["worker"]["connection"].poll(1)
			for job in jobs.values():
				if (not "stopped" in job) and job["worker"]["connection"].poll():
					job.update(job["worker"]["connection"].recv())
					free_workers.put(job["worker"])
					job["worker"] = None

		#submit last batch
		if len(batch) > 0:
			job = {}
			jobs[jobid] = job
			job["started"] = time.time()
			job["worker"] = free_workers.get()
			job["worker"]["connection"].send(batch)
			batch = []

		#wait for all jobs to finish
		while len(jobs) > 0:
			#for job in jobs.itervalues():
				#print("waiting for " + str(job["worker"]["process"].pid))
			if (self.job_is_finished(jobs[min(jobs.keys())], timeout = 1.0)):
				minjobid = min(jobs.keys())
				if (not "stopped" in jobs[minjobid]) and jobs[minjobid]["worker"]["connection"].poll():
					jobs[minjobid].update(jobs[minjobid]["worker"]["connection"].recv())
					free_workers.put(jobs[minjobid]["worker"])
					jobs[minjobid]["worker"] = None

				if "error" in jobs[minjobid]:
					self.request_stop.set()
					#shutdown workers
					for worker in workers:
						try:
							worker["connection"].close()
							worker["process"].join()
						except Exception as e:
							pass
					raise jobs[minjobid]["error"]

				for r in jobs[minjobid]["results"]:
					yield r
				del jobs[minjobid]

		#shutdown workers
		for worker in workers:
			try:
				worker["connection"].send(None) #send shutdown command
				worker["connection"].close()
				worker["process"].join()
			except Exception as e:
				pass

	def _work(self, conn, context_func):
		if context_func != None:
			try:
				with context_func() as c:
					batch = conn.recv()
					while batch != None and not self.request_stop.is_set():
						self._map_batch(conn, batch, c)
						batch = conn.recv()
			except Exception as e: #worker not initialized, return error for all incoming jobs
				batch = conn.recv()
				while batch != None and not self.request_stop.is_set():
					jobinfo = {}
					jobinfo["error"] = e
					jobinfo["stopped"] = time.time()
					conn.send(jobinfo)
					batch = conn.recv()
		else:
			batch = conn.recv()
			while batch != None and not self.request_stop.is_set():
				self._map_batch(conn, batch)
				batch = conn.recv()
		conn.close()

	def _map_batch(self, conn, batch, context = None):
		results = []
		for value in batch:
			if self.request_stop.is_set():
				jobinfo = {}
				jobinfo["error"] = Exception("stop requested")
				jobinfo["stopped"] = time.time()
				conn.send(jobinfo)
				return
			try:
				if context:
					results.append(self.map_func(value, context))
				else:
					results.append(self.map_func(value))
			except Exception as e:
				traceback.print_stack()
				print(e)
				jobinfo = {}
				jobinfo["error"] = e
				jobinfo["stopped"] = time.time()
				conn.send(jobinfo)
				return

		jobinfo = {}
		jobinfo["stopped"] = time.time()
		jobinfo["results"] = results
		#print("done:" + str(multiprocessing.current_process().pid))
		conn.send(jobinfo)
