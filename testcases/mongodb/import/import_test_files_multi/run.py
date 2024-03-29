import os
from datetime import datetime
from re import L

from EYPIBaseTest import EYPIBaseTest

class PySysTest(EYPIBaseTest):
	def __init__ (self, descriptor, outsubdir, runner):
		EYPIBaseTest.__init__(self, 'INGEST', descriptor, outsubdir, runner)
		self.files_to_process = 0

	def execute(self):
		self.drop_and_recreate_collection()

		thread_files = {}
		THREAD_COUNT = int(self.project.TEST_THREADS)
		for thread_index in range(THREAD_COUNT):
			thread_files[thread_index] = []
		test = self.project.TEST_FILE_SIZE
		data_dir = os.path.expanduser(os.path.join(self.project.DATA_DIR, 'test_files', test))
		index = 0
		for filename in os.listdir(data_dir):
			file = os.path.join(data_dir, filename)
			thread_files[index % THREAD_COUNT].append(file)
			index += 1

		for thread_index in thread_files.keys():
			files = thread_files[thread_index]
			self.files_to_process += len(files)

		drop_collection = True
		for thread_index in thread_files.keys():
			files = thread_files[thread_index]
			args = {}
			args['files'] = files
			args['thread_index'] = thread_index
			self.startBackgroundThread( f"Import file", self.import_file_proc, args)
			self.wait(2.0)
			drop_collection = False

		while self.files_to_process > 0:
			self.log.info(f'Files to process {self.files_to_process}')
			self.wait(5.0)

	def import_file_proc(self, stopping, **kwargs):
		files = kwargs['files']
		thread_index = kwargs['thread_index']

		for file in files:
			self.log.info(f'{thread_index}: Importing {file}')
			start = datetime.now()
			self.importFileMongoImport(file, f"records", dropCollection=False)
			time_taken = datetime.now() - start
			self.log.info(f'Imported {file} in {time_taken.total_seconds()}s')
			drop_collection = False
			self.files_to_process -= 1

	def drop_and_recreate_collection(self):
		db = self.get_db_connection()
		coll = db.records
		field_names = ['Year',
		          'Period',
				  'EntityName',
				  'EntityVATID',
				  'Inv.Date',
				  'Reportingperiod',
				  'Netamount(repcurr)',
				  'VATamount(repcurr)',
				  'Tx.Code',
				  'Tx.CodeDesc',
				  'Businesspartnernumber',
				  'Businesspartnername',
				  'Businesspartnercountry',
				  'Glaccount',
				  'Glaccountdescription',
				  'EY Tx.CodeDesc'
				  ]

		coll.drop()
		wildcard = {}
		for field_name in field_names:
			wildcard[field_name] = 1
		coll.create_index('$**', wildcardProjection = wildcard)


	def validate(self):
		pass
