import os
from datetime import datetime

from EYPIBaseTest import EYPIBaseTest

class PySysTest(EYPIBaseTest):
	def __init__ (self, descriptor, outsubdir, runner):
		EYPIBaseTest.__init__(self, 'INGEST', descriptor, outsubdir, runner)

	def execute(self):
		
		drop_collection = True
		test = 'medium'
		data_dir = os.path.expanduser(os.path.join(self.project.DATA_DIR, 'test_files', test))
		for filename in os.listdir(data_dir):
			file = os.path.join(data_dir, filename)
			self.log.info(f'Importing {file}')
			start = datetime.now()
			self.importFileMongoImport(file, f"records", dropCollection=drop_collection)
			time_taken = datetime.now() - start
			self.log.info(f'Imported {file} in {time_taken.total_seconds()}s')
			drop_collection = False

	def validate(self):
		pass
