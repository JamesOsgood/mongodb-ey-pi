import os
from datetime import datetime

from EYPIBaseTest import EYPIBaseTest

class PySysTest(EYPIBaseTest):
	def __init__ (self, descriptor, outsubdir, runner):
		EYPIBaseTest.__init__(self, 'INGEST_SAMPLE', descriptor, outsubdir, runner)

	def execute(self):
		
		dir = os.path.expanduser(os.path.join(self.project.DATA_DIR, 'sample', 'processed'))
		for instance_id, file in self.getFilesToProcess(dir):
			start = datetime.now()
			self.importFileMongoImport(file, f"records_{instance_id}")
			time_taken = datetime.now() - start
			self.write_test_result(instance_id, time_taken.total_seconds())

	def validate(self):
		pass
