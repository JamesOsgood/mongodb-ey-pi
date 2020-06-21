import os
from datetime import datetime

from EYPIBaseTest import EYPIBaseTest

class PySysTest(EYPIBaseTest):
	def __init__ (self, descriptor, outsubdir, runner):
		EYPIBaseTest.__init__(self, 'CONCURRENCY', descriptor, outsubdir, runner)

	def execute(self):
		
		self.build_eypi_dotnet()

		# Start all threads
		self.startBackgroundThread( f"Download file", self.download_file, {})
		self.startBackgroundThread( f"Import file", self.import_file, {})
		self.startBackgroundThread( f"Update", self.update, {})
		self.startBackgroundThread( f"Paginate", self.paginate, {})

		# Give it a couple of minutes
		seconds_to_wait = 120
		while seconds_to_wait >= 0:
			self.wait(10)
			seconds_to_wait -= 10

	def download_file(self, stopping, **kwargs):
		instances = [1]
		while not stopping.is_set():
			for instance_id in instances:
				start = datetime.now()
				coll = f"records_{instance_id}"
				self.downloadFile(coll, os.path.join(self.output, coll + ".csv"))
				time_taken = datetime.now() - start
				self.log.info(f"Download of {instance_id}GB file took {time_taken.total_seconds()} seconds")
				if stopping.is_set():
					break

	def import_file(self, stopping, **kwargs):
		conn_str = self.project.CONNECTION_STRING_MONGODB_ALT.replace("~", "=")
		instances = [1, 2]
		dir = os.path.expanduser(os.path.join(self.project.DATA_DIR, 'processed'))
		while not stopping.is_set():
			for instance_id, file in self.getFilesToProcess(dir):
				start = datetime.now()
				if instance_id in instances:
					self.importFileMongoImport(file, f"records_{instance_id}", connectionString=conn_str)
					time_taken = datetime.now() - start
					self.log.info(f"Import of {instance_id}GB file took {time_taken.total_seconds()} seconds")
					if stopping.is_set():
						break

	def paginate(self, stopping, **kwargs):
		while not stopping.is_set():
			for instance_id in self.getInstanceIds():
				test_args = { "instance_id" : instance_id, 
				"page_size" : 100, 
				"pages_to_skip" : 3, 
				"wait_time" : 500, 
				"iterations" : 10}

				self.run_eypi_dotnet("paginate", test_args)
				if stopping.is_set():
					break

				self.wait(2.0)

	def update(self, stopping, **kwargs):
		while not stopping.is_set():
			instance_ids = self.getInstanceIds()
			for instance_id in instance_ids:
				test_args = { "instance_id" : str(instance_id), 
					"wait_time" : 500, 
					"iterations" : 10}

				self.run_eypi_dotnet("update", test_args)
				if stopping.is_set():
					break

				self.wait(2.0)

	def validate(self):
		pass
