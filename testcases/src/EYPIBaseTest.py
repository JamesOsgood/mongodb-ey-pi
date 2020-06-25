
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
		self.test_run = self.project.TEST_RUN
		self.connectionStrings = {}

		self.connectionStrings['MONGODB_M30'] = self.project.CONNECTION_STRING_MONGODB_M30.replace("~", "=")
		self.connectionStrings['MONGODB_M40'] = self.project.CONNECTION_STRING_MONGODB_M40.replace("~", "=")
		self.connectionStrings['COSMOSDB_1000'] = self.project.CONNECTION_STRING_COSMOSDB_1000.replace("~", "=")

		self.connectionString = self.connectionStrings[self.test_run]

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
	
	def importFileMongoImport(self, filePath, collection, dropCollection=True, connectionString = None, ignore_blanks=False):

		if not connectionString:
			connectionString = self.connectionString

		args = []
		args.append('--headerline')
		if dropCollection:	
			args.append('--drop')

		if ignore_blanks:
			args.append('--ignoreBlanks')
		args.append('--type=csv')
		args.append('--numInsertionWorkers=8')
		args.append(f'--collection={collection}')
		args.append(f'--file={filePath}')
		args.append(f'--uri="{connectionString}"')

		command = self.project.MONGOIMPORT
		self.log.info("%s %s" % (command, " ".join(args)))
		
		self.startProcess(command, args, state=FOREGROUND, stdout='mongoimport_out.log', stderr='mongoimport_err.log', timeout=360000 )

	def importFileBatchRead(self, filePath, collection, dropCollection=True, connectionString = None, ignore_blanks = True):

		self.log.info(f'Processing {filePath}')
		if not connectionString:
			connectionString = self.connectionString

		client = MongoClient(connectionString)
		coll = client.get_database()[collection]
		if dropCollection:
			coll.drop()

		# Open the file
		headers = None
		batch = []
		BATCH_SIZE = 10000
		line_count = 0
		with open(filePath) as f:
			for line in f.readlines():
				values = line.split(',')
				if not headers:
					headers = values
					for index in range(len(headers)):
						header = headers[index]
						headers[index] = header.replace('.', '_')
				else:
					doc = {}
					for index in range(len(headers)):
						value = values[index].strip()
						if ignore_blanks:
							if len(value) > 0:
								doc[headers[index]] = value
						else:
							doc[headers[index]] = value
					if len(doc) > 0:
						batch.append(doc)
						if len(batch) == BATCH_SIZE:
							coll.insert_many(batch)
							batch = []
				line_count += 1
				if line_count % BATCH_SIZE == 0:
					self.log.info(f'Done {line_count}')

		if len(batch) == BATCH_SIZE:
			coll.insert_many(batch)
			batch = []

	def downloadFile(self, collection, output_path, connectionString = None):

		if not connectionString:
			connectionString = self.connectionString

		fields="AP_AR,Year,Period,EntityCode,EntityName,EntityVATID,ReportingCountry,Inv.Date,PostingDate,Doc.No,Invoiceno,Reportingperiod,Reportingcurrency,Netamount(repcurr),VATamount(repcurr),Grossamount(repcurr),Globalcurrency,Netamount(globalcurr),VATamount(globalcurr),Grossamount(globalcurr),Tx.Code,Tx.CodeDesc,VATRate,VATCategory,EYTaxCodeNET,EYTaxCodeVAT,Sales_Purchase,Businesspartnernumber,Businesspartnername,BusinesspartnerVATID,Businesspartnercountry,Periodicity,Duedate,Transactiontype,BusinesspartnerAddress,Businesspartnerpostalcode,Comments,Glaccount,Glaccountdescription,Net Amt (Source),VAT Amt (Source),Gross Amt (Source),Transaction currency,Fx Rate,EY Tx.Code,EY Tx.CodeDesc,Invoice_Credit note,Services_Goods,Description,Country of origin"

		args = []
		args.append('--type=csv')
		args.append(f'--collection={collection}')
		args.append(f'--fields={fields}')
		args.append(f'--out={output_path}')
		args.append(f'--uri="{connectionString}"')
		# args.append(f'--limit=100000')

		command = self.project.MONGOEXPORT
		self.log.info("%s %s" % (command, " ".join(args)))
		
		self.startProcess(command, args, state=FOREGROUND, stdout='mongoexport_out.log', stderr='mongoexport_err.log', timeout=360000 )

	def build_eypi_dotnet(self):
		args = []
		args.append('build')

		environs = {}
		for key in os.environ: environs[key] = os.environ[key]

		command = self.project.DOTNET
		self.log.info("%s %s" % (command, " ".join(args)))
		workingDir = os.path.abspath("../../../eypi_dotnet")

		index = self.get_dotnet_index()
		path_stdout = f'dotnet_build_out_{index}.log'
		path_stderr = f'dotnet_build_err_{index}.log'
		self.startProcess(command, args, state=FOREGROUND, stdout=path_stdout, stderr=path_stderr, workingDir=workingDir, environs=environs)

	def run_eypi_dotnet(self, test_name, test_args):
		args = []
		args.append('run')
		args.append(f"--uri={self.connectionString}")
		database_name = self.get_database_name_from_uri(self.connectionString)
		args.append(f"--database={database_name}")
		args.append(f"--test_run={self.test_run}")
		args.append(f"--test_name={test_name}")
		json_args = json.dumps(test_args)
		args.append(f"--test_args={test_args}")

		environs = {}
		for key in os.environ: environs[key] = os.environ[key]

		command = self.project.DOTNET
		self.log.info("%s %s" % (command, " ".join(args)))
		workingDir = os.path.abspath("../../../eypi_dotnet")

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

	def get_database_name_from_uri(self, uri):
		client = MongoClient(uri)
		return client.get_database().name

	def getInstanceIds(self):
		str_instances = f'[{self.project.TEST_INSTANCE_IDS}]'
		return eval(str_instances)

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
			'test_run' : self.test_run,
			'test_id' : self.test_id, 
			'instance_id' : instance_id,
			'ts' : datetime.now(), 
			'time_taken' : time_taken }
		test_results.insert_one(doc)

				
		
