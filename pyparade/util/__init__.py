# coding=utf8
import multiprocessing, time, math
from threading import Event, Thread

def sstr(obj):
	""" converts any object to str, if necessary encodes unicode chars """
	try:
		return str(obj)
	except UnicodeEncodeError:
		return unicode(obj).encode('utf-8')

class ParMap(object):
	"""Parallel executes a map in several threads"""
	def __init__(self, map_func, num_workers=multiprocessing.cpu_count()):
		super(ParMap, self).__init__()
		self.map_func = map_func
		self.num_workers = num_workers
		self.jobs = {}
		self.request_stop = Event()
		self.chunksize = 1

	def stop(self):
		self.request_stop.set()

	def map(self, iterable):
		jobid = 0
		batch = []
		last_processing_times = []

		for value in iterable:
			#wait while all jobs are processing
			while len(filter(lambda job: job["thread"].is_alive(), self.jobs.itervalues())) >= self.num_workers:
				time.sleep(0.1)

			#yield results while leftmost batch is ready
			while len(self.jobs) > 0 and not self.jobs[min(self.jobs.iterkeys())]["thread"].is_alive(): 
				minjobid = min(self.jobs.iterkeys())

				#update optimal chunksize based on 10*workers last batch processing times
				while len(last_processing_times) > 10*self.num_workers:
					last_processing_times.pop(0)

				last_processing_times.append((self.jobs[minjobid]["stopped"] - self.jobs[minjobid]["started"])/self.chunksize)
				avg_processing_time = sum(last_processing_times)/len(last_processing_times)

				slow_start_weight = 0.8 - 0.5*len(last_processing_times)/(10*self.num_workers) #update chunksize slowly in the beginning
				self.chunksize = int(math.ceil(slow_start_weight*self.chunksize + (1.0-slow_start_weight)*10/avg_processing_time)) #batch should take 10s to calculate

				if "error" in self.jobs[minjobid]:
					raise self.jobs[minjobid]["error"]

				for r in self.jobs[minjobid]["results"]:
					yield r
				del self.jobs[minjobid]


			#start new job if batch full
			batch.append(value)

			if len(batch) >= self.chunksize:
				if len(self.jobs) > 10*self.num_workers: #do not start jobs for more than 10*workers batches ahead to save memory
					time.sleep(0.1)
				else:
					job = {}
					self.jobs[jobid] = job
					job["thread"] = Thread(target = self._map_batch, args=(jobid, batch))
					job["started"] = time.time()
					job["thread"].start()

					batch = []
					jobid += 1

		job = {}
		self.jobs[jobid] = job
		job["thread"] = Thread(target = self._map_batch, args=(jobid, batch))
		job["started"] = time.time()
		job["thread"].run()
		batch = []

		while len(self.jobs) > 0:
			if not self.jobs[min(self.jobs.iterkeys())]["thread"].is_alive(): 
				minjobid = min(self.jobs.iterkeys())
				if "error" in self.jobs[minjobid]:
					raise self.jobs[minjobid]["error"]

				for r in self.jobs[minjobid]["results"]:
					yield r
				del self.jobs[minjobid]

	def _map_batch(self, jobid, batch):
		results = []
		for value in batch:
			if self.request_stop.is_set():
				self.jobs[jobid]["error"] = StandardError("stop requested")
				self.jobs[jobid]["stopped"] = time.time()
				return
			try:
				results.append(self.map_func(value))
			except Exception, e:
				self.jobs[jobid]["error"] = e
				self.jobs[jobid]["stopped"] = time.time()
				return
		self.jobs[jobid]["stopped"] = time.time()
		self.jobs[jobid]["results"] = results
