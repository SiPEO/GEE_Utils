import gevent
import ee
import dill
import os
import sys
import time
import traceback
import json
import copy

# will need when using outside of 
# from gevent import monkey
# monkey.patch_all()

from gevent.fileobject import FileObjectThread
from gevent.queue import Queue, Empty

dill.settings['recurse'] = True

class TimeoutException(Exception):
	pass

class RetryExceededException(Exception):
	pass

class TaskFailedException(Exception):
	pass

class DuplicateTaskException(Exception):
	pass

class GEETaskManager(object):

	def __init__(self, n_workers=5, work_forever=False, max_retry=1, wake_on_task=False, process_timeout=14400, log_file='save_state.pkl'):
		self.max_queue_size = 25
		self.task_queue = Queue(maxsize=self.max_queue_size)
		self.retry_queue = Queue(maxsize=n_workers*max_retry)
		self.max_retry = max_retry
		self.worker = self._default_worker
		self._monitor = self._default_monitor
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
		self.task_timeout = process_timeout

		self._load_log_file()

	def _load_log_file(self):
		if os.path.exists(self.log_file):
    		# Reload the old state
			f = open(self.log_file, 'rb')
			f.seek(0)
			self.task_log = dill.loads( f.read() )
			f.close()

	def _validate(self):
		if self.greenlets is None:
			raise Exception("No workers have been registered!")

	def current_time_s(self):
		return int(time.time())

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

		self.task_log[task_def['id']]['start_time'] = self.current_time_s()

		waiting = 0
		while g_task.status()['state'] in ['UNSUBMITTED', 'READY']:
			gevent.sleep(20) # Sleep for 20 seconds before checking the task again
			waiting += 20

			if waiting > 60:
				raise TimeoutException("Task [{}] failed to start on GEE platform".format(task_def['id']))

		while g_task.status()['state'] in ['RUNNING']:
			print("Task [{}] still processing...".format(task_def['id']))
			gevent.sleep(60) # Only check the status of a task every 1 minutes at most

			if self.current_time_s() - self.task_log[task_def['id']]['start_time'] >= self.task_timeout:
				raise TimeoutException("Task [{}] has timed out, processing took too long".format(task_def['id']))

		if not g_task.status()['state'] in ['COMPLETED']:
			err = ''
			if 'error_message' in g_task.status():
				self.task_log[task_def['id']]['error'] = g_task.status()['error_message']
				err = self.task_log[task_def['id']]['error']

			raise TaskFailedException("Task [{}] failed to complete successfully - {}".format(task_def['id'], err))
		else:
			self.task_log[task_def['id']]['done'] = True

	def _worker(self, worker_no):
		self.greenlet_states[worker_no] = True

		while True:
			try:
				args = self.retry_queue.get_nowait()
			except Empty:
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
						self.greenlet_states[worker_no] = False
						return

			try:
				self.worker(args)
			except (TimeoutException, TaskFailedException) as e:
				if args['id'] in self.task_log and self.task_log[args['id']]['retry'] < self.max_retry - 1:
					self._retry_task(args) # We cannot wait for this task to be queued as it blocks everything then if the queue is full
				else:
					print("Task [{}] has failed to complete".format(args['id']))
			except (DuplicateTaskException) as e:
				print("Task [{}] is a duplicate of an already completed task".format(args['id']))
			except Exception as e:
				print("Another exception occured")
				traceback.print_exc()

	def monitor(self):
		self.monitor_running = True
		while self.n_running_workers > 0:
			self._monitor(copy.deepcopy(self.task_log))
			gevent.sleep(60)

		self.monitor_running = False
		print("Monitor has quit as there are no more workers")

	def _default_monitor(self, task_log):
		# Save the state to ensure we can recover in case of a crash
		f_raw = open(self.log_file, 'w')
		with FileObjectThread(f_raw, 'w') as handle:
			dill.dump(self.task_log, handle)

		f_raw.close()

		gevent.sleep(60)

	def _task_can_run(self, task_def):
		assert isinstance(task_def, dict)

		if  task_def['id'] in self.task_log and \
			('done' in self.task_log[task_def['id']] or self.task_log[task_def['id']]['retry'] >= self.max_retry - 1):
			return False

		return True

	def _retry_task(self, task_def):

		if self._task_can_run(task_def):
			self.retry_queue.put_nowait(task_def)
			print("Retrying Task {}".format(task_def['id']))
		else:
			print("Task {} was not queued as it has already completed or has been retried too often.".format(task_def['id']))

		if self.wake_on_task:
			self._start_greenlets()

	def add_task(self, task_def, blocking=False):

		if self._task_can_run(task_def):
			if blocking:
				self.task_queue.put(task_def)
			else:
				self.task_queue.put_nowait(task_def)

			print("Queued Task {}".format(task_def['id']))
		else:
			print("Task {} was not queued as it has already completed or has been retried too often.".format(task_def['id']))

		if self.wake_on_task:
			self._start_greenlets()

	def _queue_full(self):
        # We always need at least n_workers worth of open slots in the queue in case of retries
		return self.task_queue.qsize() >= self.max_queue_size

	def _start_greenlets(self):
		if self.n_running_workers < self.n_workers:
			n_tasks = self.task_queue.qsize()
			empty_slots = [i for i, g in enumerate(self.greenlet_states) if g is False]

			for i in range(min(n_tasks, len(empty_slots))):
				if self.greenlets is None:
					self.greenlets = [None for i in range(self.n_workers)]

				worker_idx = empty_slots[i]
				self.greenlets[worker_idx] = gevent.spawn(self._worker, worker_idx)
				self.n_running_workers += 1

		if self.monitor_greenlet is None or self.monitor_running is False:
			self.monitor_greenlet = [gevent.spawn(self.monitor)]

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

	def set_task_log(self, task_log):
		self.task_log = copy.deepcopy(task_log)

	def start(self, blocking=True):
		self._start_greenlets()

		if blocking:
			self.wait_till_done()

	def wait_till_done(self):
		self._validate()
		gevent.joinall(self.monitor_greenlet + self.greenlets)
		self.n_running_workers = 0
