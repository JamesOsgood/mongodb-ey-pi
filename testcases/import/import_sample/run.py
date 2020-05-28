import os

from EYPIBaseTest import EYPIBaseTest

class PySysTest(EYPIBaseTest):

	def execute(self):
		path = os.path.expanduser(os.path.join(self.project.DATA_DIR, "sample_10k.csv"))
		self.importFileMongoImport(path, "records")

	def validate(self):
		pass
