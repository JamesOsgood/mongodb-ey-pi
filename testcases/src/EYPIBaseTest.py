
import os
from datetime import datetime
import json
import re

from pymongo import MongoClient
from pysys.basetest import BaseTest
from pysys.constants import FOREGROUND
from pysys.constants import BACKGROUND

class EYPIBaseTest(BaseTest):
	def __init__ (self, test_id, descriptor, outsubdir, runner):
		BaseTest.__init__(self, descriptor, outsubdir, runner)

		self.test_id = test_id
		self.db_connection = None
		self.connectionString = self.project.MONGODB_CONNECTION_STRING.replace("~", "=")

		# Message batching
		self.currentMessageBatch = []
		self.MESSAGE_BATCH_SIZE = 10000
		self.messages_processed = 0

		self.defaultToDate = datetime(2019,9,1)
		self.dotnet_index = -1
		self.PROCESS_TIMEOUT=60.0

	def get_dotnet_index(self):
		self.dotnet_index += 1
		return self.dotnet_index

	# open db connection
	def get_db_connection(self):
		if self.db_connection is None:
			self.log.info("Connecting to: %s" % self.connectionString)
			client = MongoClient(self.connectionString)
			self.db_connection = client.get_database()

		return self.db_connection
	
	def importFileMongoImport(self, filePath, collection):

		args = []
		args.append('--headerline')
		args.append('--drop')
		args.append('--type=csv')
		args.append('--numInsertionWorkers=4')
		args.append(f'--collection={collection}')
		args.append(f'--file={filePath}')
		args.append(f'--uri="{self.project.MONGODB_CONNECTION_STRING.replace("~", "=")}"')

		command = self.project.MONGOIMPORT
		self.log.info("%s %s" % (command, " ".join(args)))
		
		self.startProcess(command, args, state=FOREGROUND, stdout='mongoimport_out.log', stderr='mongoimport_err.log', timeout=3600 )

	def downloadFile(self, collection, output_path):

		fields="AP_AR,Year,Period,EntityCode,EntityName,EntityVATID,ReportingCountry,Inv.Date,PostingDate,Doc.No,Invoiceno,Reportingperiod,Reportingcurrency,Netamount(repcurr),VATamount(repcurr),Grossamount(repcurr),Globalcurrency,Netamount(globalcurr),VATamount(globalcurr),Grossamount(globalcurr),Tx.Code,Tx.CodeDesc,VATRate,VATCategory,EYTaxCodeNET,EYTaxCodeVAT,Sales_Purchase,Businesspartnernumber,Businesspartnername,BusinesspartnerVATID,Businesspartnercountry,Periodicity,Duedate,Transactiontype,BusinesspartnerAddress,Businesspartnerpostalcode,Comments,Glaccount,Glaccountdescription,Net Amt (Source),VAT Amt (Source),Gross Amt (Source),Transaction currency,Fx Rate,EY Tx.Code,EY Tx.CodeDesc,Invoice_Credit note,Services_Goods,Description,Country of origin"

		args = []
		args.append('--type=csv')
		args.append(f'--collection={collection}')
		args.append(f'--fields={fields}')
		args.append(f'--out={output_path}')
		args.append(f'--uri="{self.project.MONGODB_CONNECTION_STRING.replace("~", "=")}"')
		# args.append(f'--limit=100000')

		command = self.project.MONGOEXPORT
		self.log.info("%s %s" % (command, " ".join(args)))
		
		self.startProcess(command, args, state=FOREGROUND, stdout='mongoimport_out.log', stderr='mongoimport_err.log', timeout=3600 )

	def build_eypi_dotnet(self):
		args = []
		args.append('build')

		environs = {}
		for key in os.environ: environs[key] = os.environ[key]

		command = self.project.DOTNET
		self.log.info("%s %s" % (command, " ".join(args)))
		workingDir = os.path.abspath("../../eypi_dotnet")

		index = self.get_dotnet_index()
		path_stdout = f'dotnet_build_out_{index}.log'
		path_stderr = f'dotnet_build_err_{index}.log'
		self.startProcess(command, args, state=FOREGROUND, stdout=path_stdout, stderr=path_stderr, workingDir=workingDir, environs=environs)

	def run_eypi_dotnet(self, test_name, test_args):
		args = []
		args.append('run')
		args.append(f"--uri={self.connectionString}")
		args.append(f"--test_name={test_name}")
		json_args = json.dumps(test_args)
		args.append(f"--test_args={test_args}")

		environs = {}
		for key in os.environ: environs[key] = os.environ[key]

		command = self.project.DOTNET
		self.log.info("%s %s" % (command, " ".join(args)))
		workingDir = os.path.abspath("../../eypi_dotnet")

		index = self.get_dotnet_index()
		path_stdout = f'dotnet_run_out_{index}.log'
		path_stderr = f'dotnet_run_err_{index}.log'
		self.startProcess(command, args, state=FOREGROUND, stdout=path_stdout, stderr=path_stderr, workingDir=workingDir, environs=environs)

	def run_eypi_dotnet_async(self, command):
		args = []
		args.append('run')
		args.append(f"--uri={self.connectionString}")
		args.append(f"--command={command}")

		environs = {}
		for key in os.environ: environs[key] = os.environ[key]

		command = self.project.DOTNET
		self.log.info("%s %s" % (command, " ".join(args)))
		workingDir = os.path.abspath("../../eypi_dotnet")

		index = self.get_dotnet_index()
		path_stdout = f'dotnet_run_out_{index}.log'
		path_stderr = f'dotnet_run_err_{index}.log'
		return self.startProcess(command, args, state=BACKGROUND, stdout=path_stdout, stderr=path_stderr, workingDir=workingDir, environs=environs)

	def run_eypi_dotnet_commands(self, commands):

		processes = []
		for command in commands:
			p = self.run_eypi_dotnet_async(command)
			processes.append(p)

		for p in processes:
			self.waitProcess(p, self.PROCESS_TIMEOUT)


	def getInstanceIds(self):
		return [1, 2, 5, 10]

	def getFilesToProcess(self, data_dir = None):
		if not data_dir:
			data_dir = os.path.expanduser(self.project.DATA_DIR)

		p = re.compile('.*_(\d*)gb.*')
		filepaths = []
		for file in os.listdir(data_dir):
			if file.endswith(".csv"):
				m = p.match(file)
				size = int(m[1])
				filepaths.append( (size, os.path.join(data_dir, file)))

		return filepaths

	def write_test_result(self, instance_id, time_taken):
		db = self.get_db_connection()
		test_results = db.test_results
		
		doc = { 
			'test_id' : self.test_id, 
			'instance_id' : instance_id,
			'ts' : datetime.now(), 
			'time_taken' : time_taken }
		test_results.insert_one(doc)

				
		
