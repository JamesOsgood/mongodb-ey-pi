using System;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Linq;
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
                var db = base.ConnectToMongoDB(connectionString, "eypi");
                var collection = db.GetCollection<BsonDocument>(String.Format("records_{0}", instance_id));
                var docs = collection.AsQueryable().Sample(100).ToList();
                var ids = new List<Object>();
                foreach( var doc in docs )
                {
                    var doc_id = doc["_id"];
                    ids.Add(doc_id);
                }

                var sw = Stopwatch.StartNew();

                var filter_doc = new BsonDocument("_id", 
                    new BsonDocument("$in", new BsonArray(ids)));

                int rand = new Random().Next();
                var update = Builders<BsonDocument>.Update.Set("Glaccountdescription", "Rand_" + rand.ToString());
                var result = collection.UpdateMany(filter_doc, update);     
                Console.WriteLine(String.Format("Matched {0}, updated {1}", result.MatchedCount, result.ModifiedCount));
                sw.Stop();

                // Insert result
                base.WriteTestResult(db, "UPDATE", instance_id, sw.ElapsedMilliseconds);
                
                Thread.Sleep(wait_time);
            }
        }
    }
}
