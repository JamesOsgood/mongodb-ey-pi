import os

from EYPIBaseTest import EYPIBaseTest

class PySysTest(EYPIBaseTest):

	def execute(self):
		self.build_eypi_dotnet()
		
		commands = ['paginate', 'paginate', 'paginate','paginate']
		self.run_eypi_dotnet_commands(commands)

	def validate(self):
		pass
