import os
from datetime import datetime

from EYPIBaseTest import EYPIBaseTest

class PySysTest(EYPIBaseTest):
	def __init__ (self, descriptor, outsubdir, runner):
		EYPIBaseTest.__init__(self, 'INGEST', descriptor, outsubdir, runner)

	def execute(self):
		
		# instances = [1,2,5,10]
		instances = [1,2,5,10]
		dir = os.path.expanduser(os.path.join(self.project.DATA_DIR, 'processed'))
		for instance_id, file in self.getFilesToProcess(dir):
			start = datetime.now()
			if instance_id in instances:
				self.importFileMongoImport(file, f"records_{instance_id}")
				time_taken = datetime.now() - start
				self.write_test_result(instance_id, time_taken.total_seconds())

	def validate(self):
		pass
