import os

from EYPIBaseTest import EYPIBaseTest

class PySysTest(EYPIBaseTest):

	def execute(self):
		self.build_eypi_dotnet()
		
		test_name = "paginate"
		test_args = { "page_size" : 100, "pages_to_skip" : 3, "wait_time" : 500, "iterations" : 10}
		self.run_eypi_dotnet(test_name, test_args)

	def validate(self):
		pass
