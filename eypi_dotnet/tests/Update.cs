using System;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Diagnostics;
using System.Threading;
using System.Collections.Generic;

namespace eypi_dotnet.tests
{
    class Update : MongoDBTest
    {
        public override void RunTest( string connectionString, Dictionary<string, object> testArgs )
        {
            string instance_id = Convert.ToString(testArgs["instance_id"]);
            int totalRuns = Convert.ToInt32(testArgs["iterations"]);
            int wait_time = Convert.ToInt32(testArgs["wait_time"]);

            for ( int i = 0; i < totalRuns; i++)
            {
                var sw = Stopwatch.StartNew();
                var db = base.ConnectToMongoDB(connectionString, "eypi");
                var collection = db.GetCollection<BsonDocument>(String.Format("records_{0}", instance_id));

                var filter = Builders<BsonDocument>.Filter.Eq("EntityVATID", "45676576");
                var update = Builders<BsonDocument>.Update.Set("EntityVATID", "45676576");
                var result = collection.UpdateOne(filter, update);                
                sw.Stop();

                // Insert result
                base.WriteTestResult(db, "UPDATE", instance_id, sw.ElapsedMilliseconds);
                
                Thread.Sleep(wait_time);
            }
        }
    }
}
