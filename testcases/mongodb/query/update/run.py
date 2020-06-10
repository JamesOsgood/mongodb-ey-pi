import os

from EYPIBaseTest import EYPIBaseTest

class PySysTest(EYPIBaseTest):
	def __init__ (self, descriptor, outsubdir, runner):
		EYPIBaseTest.__init__(self, 'QUERY-UPDATE', descriptor, outsubdir, runner)

	def execute(self):
		self.build_eypi_dotnet()
		
		test_name = "update"
		instance_ids = self.getInstanceIds()
		for instance_id in instance_ids:
			test_args = { "instance_id" : str(instance_id), 
				"wait_time" : 500, 
				"iterations" : 10}

			self.run_eypi_dotnet(test_name, test_args)

	def validate(self):
		pass
