# coding=utf8
import multiprocessing, time, math, traceback, Queue
from multiprocessing import Process
import multiprocessing

def sstr(obj):
	""" converts any object to str, if necessary encodes unicode chars """
	try:
		return str(obj)
	except UnicodeEncodeError:
		return unicode(obj).encode('utf-8')

class Event(object):
	def __init__(self):
		self.handlers = []
	
	def add(self, handler):
		self.handlers.append(handler)
		return self
	
	def remove(self, handler):
		self.handlers.remove(handler)
		return self
	
	def fire(self, sender, earg=None):
		for handler in self.handlers:
			handler(sender, earg)
	
	__iadd__ = add
	__isub__ = remove
	__call__ = fire

class Timer(object):
	"""measures time"""
	def __init__(self, description):
		super(Timer, self).__init__()
		self.start = time.time()
		self.description = description

	def stop(self):
		self.end = time.time()
		self.seconds = self.end-self.start
		print(self.description + " took " + str(self.seconds) + "s.")

class ParMap(object):
	"""Parallel executes a map in several threads"""
	def __init__(self, map_func, context_func = None, num_workers=multiprocessing.cpu_count()):
		super(ParMap, self).__init__()
		self.map_func = map_func
		self.context_func = context_func
		self.num_workers = num_workers
		self.workers = []
		self.free_workers = Queue.Queue()
		self.request_stop = multiprocessing.Event()
		self.chunksize = 1

	def stop(self):
		self.request_stop.set()

	def map(self, iterable):
		#initialize
		self.workers = []
		self.free_workers = Queue.Queue()
		self.request_stop = multiprocessing.Event()
		self.chunksize = 1

		#start up workers
		for i in range(self.num_workers):
			parent_conn, child_conn = multiprocessing.Pipe()
			worker = {} 
			worker["connection"] =  parent_conn
			worker["process"] = Process(target = self._work, args=(child_conn, self.context_func))
			self.workers.append(worker)
			self.free_workers.put(worker)
			worker["process"].start()

		#process values
		jobs = {}
		jobid = 0
		batch = []
		last_processing_times = []

		for value in iterable:
			#wait as long as all workers are busy
			while self.free_workers.empty():
				minjobid = min(jobs.iterkeys())
				jobs[minjobid]["worker"]["connection"].poll(1)
				for job in jobs.itervalues():
					if (not "stopped" in job) and job["worker"]["connection"].poll():
						job.update(job["worker"]["connection"].recv())
						self.free_workers.put(job["worker"])

			#yield results while leftmost batch is ready
			while len(jobs) > 0 and ("stopped" in jobs[min(jobs.iterkeys())] or jobs[min(jobs.iterkeys())]["worker"]["connection"].poll()): 
				minjobid = min(jobs.iterkeys())
				if jobs[minjobid]["worker"]["connection"].poll():
					jobs[minjobid].update(jobs[minjobid]["worker"]["connection"].recv())
					self.free_workers.put(jobs[minjobid]["worker"])

				#update optimal chunksize based on 10*workers last batch processing times
				while len(last_processing_times) > 10*self.num_workers:
					last_processing_times.pop(0)

				last_processing_times.append((jobs[minjobid]["stopped"] - jobs[minjobid]["started"])/self.chunksize)
				avg_processing_time = sum(last_processing_times)/len(last_processing_times)

				slow_start_weight = 0.8 - 0.5*len(last_processing_times)/(10*self.num_workers) #update chunksize slowly in the beginning
				desired_chunksize = int(math.ceil(slow_start_weight*self.chunksize + (1.0-slow_start_weight)*3.0/avg_processing_time)) #batch should take 1s to calculate
				self.chunksize = min(desired_chunksize, max(10,2*self.chunksize)) #double chunksize at most every time (but allow to go to 10 directly in the beginning)

				if "error" in jobs[minjobid]:
					raise jobs[minjobid]["error"]

				for r in jobs[minjobid]["results"]:
					yield r
				del jobs[minjobid]

			#start new job if batch full
			batch.append(value)

			if len(batch) >= self.chunksize:
				if len(jobs) > 10*self.num_workers: #do not start jobs for more than 10*workers batches ahead to save memory
					time.sleep(0.1)
				else:
					job = {}
					jobs[jobid] = job
					job["started"] = time.time()
					job["worker"] = self.free_workers.get()
					job["worker"]["connection"].send(batch)

					batch = []
					jobid += 1

		#submit last batch
		job = {}
		jobs[jobid] = job
		job["started"] = time.time()
		job["worker"] = self.free_workers.get()
		job["worker"]["connection"].send(batch)
		batch = []

		#wait for all jobs to finish
		while len(jobs) > 0:
			minjobid = min(jobs.iterkeys())
			jobs[minjobid]["worker"]["connection"].poll(1)
			if "stopped" in jobs[minjobid] or jobs[minjobid]["worker"]["connection"].poll():
				if jobs[minjobid]["worker"]["connection"].poll():
					jobs[minjobid].update(jobs[minjobid]["worker"]["connection"].recv())
					self.free_workers.put(jobs[minjobid]["worker"])

				if "error" in jobs[minjobid]:
					raise jobs[minjobid]["error"]

				for r in jobs[minjobid]["results"]:
					yield r
				del jobs[minjobid]

		#shutdown workers
		for worker in self.workers:
			try:
				worker["connection"].send(None) #send shutdown command
				worker["connection"].close()
			except Exception, e:
				pass

		self.workers = []

	def _work(self, conn, context_func):
		if context_func != None:
			with context_func() as c:
				batch = conn.recv()
				while batch != None and not self.request_stop.is_set():
					self._map_batch(conn, batch, c)
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
				jobinfo["error"] = StandardError("stop requested")
				jobinfo["stopped"] = time.time()
				conn.send(jobinfo)
				return
			try:
				if context:
					results.append(self.map_func(value, context))
				else:
					results.append(self.map_func(value))
			except Exception, e:
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
		conn.send(jobinfo)
