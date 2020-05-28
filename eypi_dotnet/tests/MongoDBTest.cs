using System;
using MongoDB.Driver;

namespace eypi_dotnet.tests
{
    abstract class MongoDBTest
    {
        abstract public void RunTest(string connectionString);
    
        // Connect to MongoDB
        protected IMongoDatabase ConnectToMongoDB( string connectionString, string database )
        {
            // or use a connection string
            var client = new MongoClient(connectionString);
            return client.GetDatabase(database);
        }
    }
}   
