import os
from datetime import datetime
from re import L

from EYPIBaseTest import EYPIBaseTest

class PySysTest(EYPIBaseTest):
	def __init__ (self, descriptor, outsubdir, runner):
		EYPIBaseTest.__init__(self, 'INGEST', descriptor, outsubdir, runner)

	def execute(self):
		
		thread_files = {}
		THREAD_COUNT = 2
		for thread_index in range(THREAD_COUNT):
			thread_files[thread_index] = []
		test = 'medium'
		data_dir = os.path.expanduser(os.path.join(self.project.DATA_DIR, 'test_files', test))
		index = 0
		for filename in os.listdir(data_dir):
			file = os.path.join(data_dir, filename)
			thread_files[index % THREAD_COUNT].append(file)
			index += 1

		# for thread_index in thread_files.keys():
		# 	files = thread_files[thread_index]
		# 	self.log.info(len(files))

		drop_collection = True
		for thread_index in thread_files.keys():
			files = thread_files[thread_index]
			args = {}
			args['files'] = files
			args['drop_collection'] = drop_collection
			args['thread_index'] = thread_index
			self.startBackgroundThread( f"Import file", self.import_file_proc, args)
			self.wait(5.0)
			drop_collection = False

	
	def import_file_proc(self, stopping, **kwargs):
		files = kwargs['files']
		drop_collection = kwargs['drop_collection']
		thread_index = kwargs['thread_index']

		for file in files:
			self.log.info(f'{thread_index}: Importing {file}')
			start = datetime.now()
			self.importFileMongoImport(file, f"records", dropCollection=drop_collection)
			time_taken = datetime.now() - start
			self.log.info(f'Imported {file} in {time_taken.total_seconds()}s')
			drop_collection = False

	def validate(self):
		pass
