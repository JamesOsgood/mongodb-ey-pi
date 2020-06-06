using System;
using MongoDB.Driver;
using System.Collections.Generic;

namespace eypi_dotnet.tests
{
    abstract class MongoDBTest
    {
        abstract public void RunTest(string connectionString, string testRun, Dictionary<string, object> testArgs);
    
        // Connect to MongoDB
        protected IMongoDatabase ConnectToMongoDB( string connectionString, string database )
        {
            // or use a connection string
            var client = new MongoClient(connectionString);
            return client.GetDatabase(database);
        }
    
        // Write test result
        protected void WriteTestResult(IMongoDatabase db, string testRun, string testId, string instanceId, double timeTaken)
        {
            TestResult tr = new TestResult{
                TestRun = testRun,
                TestID=testId, 
                InstanceID=instanceId,
                TS=DateTime.Now,
                TimeTaken=timeTaken};

            IMongoCollection<TestResult> coll = db.GetCollection<TestResult>("test_results");
            coll.InsertOne(tr);
        }
    }
}   
