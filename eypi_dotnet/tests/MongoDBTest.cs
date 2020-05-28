using System;
using MongoDB.Driver;
using System.Collections.Generic;

namespace eypi_dotnet.tests
{
    abstract class MongoDBTest
    {
        abstract public void RunTest(string connectionString, Dictionary<string, object> testArgs);
    
        // Connect to MongoDB
        protected IMongoDatabase ConnectToMongoDB( string connectionString, string database )
        {
            // or use a connection string
            var client = new MongoClient(connectionString);
            return client.GetDatabase(database);
        }
    }
}   
