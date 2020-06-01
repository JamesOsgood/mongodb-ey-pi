import os
from datetime import datetime

from EYPIBaseTest import EYPIBaseTest

class PySysTest(EYPIBaseTest):
	def __init__ (self, descriptor, outsubdir, runner):
		EYPIBaseTest.__init__(self, 'DOWNLOAD', descriptor, outsubdir, runner)

	def execute(self):
		
		instances = self.getInstanceIds()
		instance = [1]
		for instance_id in instances:
			start = datetime.now()
			coll = f"records_{instance_id}"
			self.downloadFile(coll, os.path.join(self.output, coll + ".csv"))
			time_taken = datetime.now() - start
			self.write_test_result(instance_id, time_taken.total_seconds())

	def validate(self):
		pass
