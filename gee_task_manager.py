import gevent
import ee
import dill
import os
from gevent import monkey
monkey.patch_all()

from gevent.fileobject import FileObjectThread
from gevent.queue import Queue, Empty

class TimeoutException(Exception):
	pass

class RetryExceededException(Exception):
	pass

class TaskFailedException(Exception):
	pass

class DuplicateTaskException(Exception):
	pass

class GEETaskManager(object):

	def __init__(self, n_workers=5, work_forever=False, max_retry=1, wake_on_task=False, log_file='save_state.pkl'):
		self.task_queue = Queue(maxsize=50)
		self.max_retry = max_retry
		self.worker = self._default_worker
		self.monitor = self._monitor
		self.monitor_running = False
		self.monitor_greenlet = None
		self.greenlets = None
		self.greenlet_states = [False for i in range(n_workers)]
		self.n_workers = n_workers
		self.work_forever = work_forever
		self.wake_on_task = wake_on_task
		self.n_running_workers = 0
		self.task_log = {}
		self.log_file = log_file

		self._load_log_file()

	def _load_log_file(self):
		if os.path.exists(self.log_file):
    		# Reload the old state
			self.task_log = dill.load( open(self.log_file, 'rb') )

	def _validate(self):
		if self.greenlets is None:
			raise Exception("No workers have been registered!")

	def _default_worker(self, task_def):
		g_task = task_def['action'](**task_def['kwargs'])

		assert g_task, "Task could not be created from factory. Something bigger is wrong."

		try:
			g_task.start()
		except:
			raise TaskFailedException("Task [{}] failed to start on GEE platform".format(task_def['id']))
		finally:
			print("Processing {}".format(task_def['id']))

		if task_def['id'] in self.task_log:
			if 'done' in self.task_log[task_def['id']] and self.task_log[task_def['id']]['done']:
				raise DuplicateTaskException("Task [{}] has already completed".format(task_def['id']))

			self.task_log[task_def['id']]['retry'] += 1
			self.task_log[task_def['id']]['task_ids'] += [g_task.id]
		else:
			self.task_log[task_def['id']] = {'retry': 0, 'task_def': task_def, 'task_ids': [g_task.id]}

		waiting = 0
		while g_task.status()['state'] in ['UNSUBMITTED', 'READY']:
			gevent.sleep(20) # Sleep for 20 seconds before checking the task again
			waiting += 20

			if waiting > 60:
				raise TimeoutException("Task [{}] failed to start on GEE platform".format(task_def['id']))

		while g_task.status()['state'] in ['RUNNING']:
			gevent.sleep(1*60) # Only check the status of a task every 1 minutes at most

		if not g_task.status()['state'] in ['COMPLETED']:
			if 'error_message' in g_task.status():
				self.task_log[task_def['id']]['error'] = g_task.status()['error_message']

			raise TaskFailedException("Task [{}] failed to complete successfully".format(task_def['id']))
		else:
			self.task_log[task_def['id']]['done'] = True

	def _worker(self, worker_no):
		self.greenlet_states[worker_no-1] = True

		while True:
			try:
				args = self.task_queue.get_nowait()
			except Empty:
				if self.work_forever:
					sleep_time = getattr(self, "worker_sleep_time", 5)
					print("Worker {} will sleep {}s".format(worker_no, sleep_time))
					gevent.sleep(sleep_time)
					continue
				else:
					print("Worker {} has no more work... Returning".format(worker_no))
					self.n_running_workers -= 1
					self.greenlet_states[worker_no-1] = False
					return

			try:
				self.worker(args)
			except (TimeoutException, TaskFailedException) as e:
				if self.task_log[args['id']]['retry'] < self.max_retry - 1:
					self.add_task(args, blocking=True) # Requeue the task for trying again later
				else:
					print("Task [{}] has failed to complete".format(args['id']))
			except Exception as e:
				print(e)

	def _monitor(self):
		self.monitor_running = True

		while self.n_running_workers > 0:
			# Save the state to ensure we can recover in case of a crash
			f_raw = open(self.log_file, 'wb')
			with FileObjectThread(f_raw, 'wb') as handle:
				dill.dump(self.task_log, handle)

			gevent.sleep(60)

		self.monitor_running = False
		print("Monitor has quit as there are no more workers")

	def add_task(self, task_def, blocking=False):
		assert isinstance(task_def, dict)

		if  task_def['id'] in self.task_log and \
			('done' in self.task_log[task_def['id']] or self.task_log[task_def['id']]['retry'] >= self.max_retry):
			print("Task {} was not queued as it has already completed.".format(task_def['id']))
			return

		if blocking:
			self.task_queue.put(task_def)
		else:
			self.task_queue.put_nowait(task_def)

		print("Queued Task {}".format(task_def['id']))

		if self.wake_on_task:
			self._start_on_task()

	def _queue_full(self):
		max_task_queue = getattr(self, "max_task_queue", 50)
		return self.task_queue.qsize() >= max_task_queue

	def _start_on_task(self):
		if self.greenlets is None:
			self.start(blocking=False)

		if self.n_running_workers < self.n_workers and self.task_queue.qsize() >= 0:
			empty_slots = [i for i, g in enumerate(self.greenlet_states) if g is False]

			for i in empty_slots:
				self.n_running_workers += 1
				self.greenlets[i] = gevent.spawn(self._worker, i+1)

		if self.monitor_running is False:
			self.monitor_greenlet = [gevent.spawn(self._monitor)]


	def wait_for_queue(self):
		queue_sleep_time = getattr(self, "queue_sleep_time", 30)
		while self._is_queue_full():
			gevent.sleep(queue_sleep_time)

	def register_worker(self, func):
		self._worker = func

	def register_monitor(self, func):
		self._monitor = func

	def get_task_log(self):
		return self.task_log

	def start(self, blocking=True):
		self.n_running_workers = self.n_workers

		self.monitor_greenlet = [gevent.spawn(self._monitor)]
		self.greenlets = [gevent.spawn(self._worker, i) for i in range(1, self.n_workers + 1)]

		if blocking:
			self.wait_till_done()

	def wait_till_done(self):
		self._validate()
		gevent.joinall(self.monitor_greenlet + self.greenlets)
		self.n_running_workers = 0
