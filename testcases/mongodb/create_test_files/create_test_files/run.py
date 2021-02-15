import os
import shutil

from EYPIBaseTest import EYPIBaseTest

class PySysTest(EYPIBaseTest):
	def __init__ (self, descriptor, outsubdir, runner):
		EYPIBaseTest.__init__(self, 'CREATE_FILE', descriptor, outsubdir, runner)

	def execute(self):
		data_file = 'Setup_Data_File_5gb_processed.csv'
		data_dir = '/Users/james/data/mongodb/eypi/processed'
		output_dir = '/Users/james/data/mongodb/eypi/test_files'
		input_path = os.path.join(data_dir, data_file)

		# self.create_test_files(input_path, os.path.join(output_dir, 'small'), 15000, 100)
		# self.create_test_files(input_path, os.path.join(output_dir, 'medium'), 810000, 10)
		self.create_test_files(input_path, os.path.join(output_dir, 'large'), 2600000, 5)

	def create_test_files(self, input_path, output_dir, line_count, files_to_create):

		try:
			shutil.rmtree(output_dir)
		except:
			pass

		os.mkdir(output_dir)
		with open(input_path) as input_file:
			header = input_file.readline()

			file_index = 0
			while file_index < files_to_create:
				self.log.info(f'Creating file {file_index}')
				with open(os.path.join(output_dir, f'test_file_{file_index}.csv'), 'w') as output_file:
					output_file.write(header)
					line_index = 0
					while line_index < line_count:
						line = input_file.readline()
						output_file.write(line)
						line_index += 1
					file_index += 1
				



		
		
	def validate(self):
		pass
