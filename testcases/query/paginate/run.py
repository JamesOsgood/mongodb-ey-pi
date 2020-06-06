import os

from EYPIBaseTest import EYPIBaseTest

class PySysTest(EYPIBaseTest):
	def __init__ (self, descriptor, outsubdir, runner):
		EYPIBaseTest.__init__(self, 'QUERY-PAGINATE', descriptor, outsubdir, runner)

	def execute(self):
		self.build_eypi_dotnet()
		
		test_name = "paginate"
		for instance_id in self.getInstanceIds():
			test_args = { "instance_id" : instance_id, 
			"page_size" : 100, 
			"pages_to_skip" : 3, 
			"wait_time" : 500, 
			"iterations" : 10}

			self.run_eypi_dotnet(test_name, test_args)

	def validate(self):
		pass
