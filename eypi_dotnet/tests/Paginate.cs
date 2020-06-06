using System;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Diagnostics;
using System.Threading;
using System.Collections.Generic;

namespace eypi_dotnet.tests
{
    class Paginate : MongoDBTest
    {
        public override void RunTest( string connectionString, string testRun, Dictionary<string, object> testArgs )
        {
            string instance_id = Convert.ToString(testArgs["instance_id"]);
            int totalRuns = Convert.ToInt32(testArgs["iterations"]);
            int page_size = Convert.ToInt32(testArgs["page_size"]);
            int pages_to_skip = Convert.ToInt32(testArgs["pages_to_skip"]);
            int wait_time = Convert.ToInt32(testArgs["wait_time"]);

            for ( int i = 0; i < totalRuns; i++)
            {
                var sw = Stopwatch.StartNew();
                var db = base.ConnectToMongoDB(connectionString, "eypi");
                var collection = db.GetCollection<BsonDocument>(String.Format("records_{0}", instance_id));
                var documents = collection.Find(new BsonDocument()).Limit(page_size).Skip(page_size * pages_to_skip).ToList();
                sw.Stop();
                // Insert result
                base.WriteTestResult(db, testRun, "PAGINATE", instance_id, sw.ElapsedMilliseconds);
                
                Thread.Sleep(wait_time);
            }
        }
    }
}
