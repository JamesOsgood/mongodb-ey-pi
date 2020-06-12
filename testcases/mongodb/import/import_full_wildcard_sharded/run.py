import os
from datetime import datetime
from pymongo import MongoClient

from EYPIBaseTest import EYPIBaseTest

class PySysTest(EYPIBaseTest):
	def __init__ (self, descriptor, outsubdir, runner):
		EYPIBaseTest.__init__(self, 'INGEST', descriptor, outsubdir, runner)

	def execute(self):
		
		# shard db
		client = MongoClient(self.connectionString)
		db = self.get_db_connection()
		dbname = db.name
		client.admin.command('enableSharding', dbname)

		instances = self.getInstanceIds()
		dir = os.path.expanduser(os.path.join(self.project.DATA_DIR, 'processed'))
		for instance_id, file in self.getFilesToProcess(dir):
			start = datetime.now()
			if instance_id in instances:
				self.drop_collection_and_create_wildcard_index(dbname, instance_id)
				collection_name = f"records_{instance_id}"
				self.log.info(f"Importing data for {collection_name}")
				self.importFileMongoImport(file, collection_name, dropCollection = False)
				time_taken = datetime.now() - start
				self.write_test_result(instance_id, time_taken.total_seconds())

	def drop_collection_and_create_wildcard_index(self, dbname, instance_id):
		client = self.get_db_connection()
		collection_name = f"records_{instance_id}"

		collection = client.get_collection(collection_name)
		self.log.info(f"Dropping collection {collection_name}")
		collection.drop()

		# shard collection
		self.log.info(f"Sharding collection {collection_name}")
		client = MongoClient(self.connectionString)
		client.admin.command('shardCollection', f'{dbname}.{collection_name}', key={'_id': "hashed"})

		self.log.info(f"Creating wildcard index for {collection_name}")
		collection.create_index("$**")

	def validate(self):
		pass
