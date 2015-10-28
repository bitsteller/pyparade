# coding=utf8
import multiprocessing, time, math, traceback
from multiprocessing import Process
import multiprocessing

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
		self.request_stop = multiprocessing.Event()
		self.chunksize = 1

	def stop(self):
		self.request_stop.set()

	def map(self, iterable):
		jobid = 0
		batch = []
		last_processing_times = []

		for value in iterable:
			#wait as long as all jobs are processing
			active_jobs = filter(lambda job: job["thread"].is_alive(), self.jobs.itervalues())
			while len(active_jobs) >= self.num_workers:
				time.sleep(0.1)
				for job in active_jobs:
					if job["connection"].poll():
						job.update(job["connection"].recv())
						active_jobs.remove(job)

			#yield results while leftmost batch is ready
			while len(self.jobs) > 0 and (not self.jobs[min(self.jobs.iterkeys())]["thread"].is_alive() or self.jobs[min(self.jobs.iterkeys())]["connection"].poll()): 
				minjobid = min(self.jobs.iterkeys())
				if self.jobs[minjobid]["connection"].poll(0.5):
					self.jobs[minjobid].update(self.jobs[minjobid]["connection"].recv())
					self.jobs[minjobid]["connection"].close()

				#update optimal chunksize based on 10*workers last batch processing times
				while len(last_processing_times) > 10*self.num_workers:
					last_processing_times.pop(0)

				last_processing_times.append((self.jobs[minjobid]["stopped"] - self.jobs[minjobid]["started"])/self.chunksize)
				avg_processing_time = sum(last_processing_times)/len(last_processing_times)

				slow_start_weight = 0.8 - 0.5*len(last_processing_times)/(10*self.num_workers) #update chunksize slowly in the beginning
				desired_chunksize = int(math.ceil(slow_start_weight*self.chunksize + (1.0-slow_start_weight)*3.0/avg_processing_time)) #batch should take 1s to calculate
				self.chunksize = min(desired_chunksize, max(10,2*self.chunksize)) #double chunksize at most every time (but allow to go to 10 directly in the beginning)

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
					parent_conn, child_conn = multiprocessing.Pipe(duplex = False)
					job["connection"] = parent_conn
					job["thread"] = Process(target = self._map_batch, args=(child_conn, batch))#Thread(target = self._map_batch, args=(jobid, batch))
					job["started"] = time.time()
					job["thread"].start()

					batch = []
					jobid += 1

		job = {}
		self.jobs[jobid] = job
		parent_conn, child_conn = multiprocessing.Pipe(duplex = False)
		job["connection"] = parent_conn
		job["thread"] = Process(target = self._map_batch, args=(child_conn, batch))# Thread(target = self._map_batch, args=(jobid, batch))
		job["started"] = time.time()
		job["thread"].start()
		batch = []

		while len(self.jobs) > 0:
			minjobid = min(self.jobs.iterkeys())
			if not self.jobs[minjobid]["thread"].is_alive() or self.jobs[minjobid]["connection"].poll():
				if self.jobs[minjobid]["connection"].poll():
					self.jobs[minjobid].update(self.jobs[minjobid]["connection"].recv())
					self.jobs[minjobid]["connection"].close()

				if "error" in self.jobs[minjobid]:
					raise self.jobs[minjobid]["error"]

				for r in self.jobs[minjobid]["results"]:
					yield r
				del self.jobs[minjobid]

	def _map_batch(self, conn, batch):
		results = []
		for value in batch:
			if self.request_stop.is_set():
				jobinfo = {}
				jobinfo["error"] = StandardError("stop requested")
				jobinfo["stopped"] = time.time()
				conn.send(jobinfo)
				conn.close()
				return
			try:
				results.append(self.map_func(value))
			except Exception, e:
				traceback.print_stack()
				print(e)
				jobinfo = {}
				jobinfo["error"] = e
				jobinfo["stopped"] = time.time()
				conn.send(jobinfo)
				conn.close()
				return

		jobinfo = {}
		jobinfo["stopped"] = time.time()
		jobinfo["results"] = results
		conn.send(jobinfo)
		conn.close()
