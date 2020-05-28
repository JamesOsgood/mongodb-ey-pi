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
        public override void RunTest( string connectionString, Dictionary<string, object> testArgs )
        {
            int TOTAL_RUNS = Convert.ToInt32(testArgs["iterations"]);
            int page_size = Convert.ToInt32(testArgs["page_size"]);
            int pages_to_skip = Convert.ToInt32(testArgs["pages_to_skip"]);
            int wait_time = Convert.ToInt32(testArgs["wait_time"]);

            long totalElapsed = 0;
            for ( int i = 0; i < TOTAL_RUNS; i++)
            {
                var sw = Stopwatch.StartNew();
                var db = base.ConnectToMongoDB(connectionString, "eypi");
                var collection = db.GetCollection<BsonDocument>("records");
                var documents = collection.Find(new BsonDocument()).Limit(page_size).Skip(page_size * pages_to_skip).ToList();
                Console.WriteLine(documents.Count);
                sw.Stop();
                totalElapsed += sw.ElapsedMilliseconds;
                Thread.Sleep(wait_time);
            }
        
            Console.Out.WriteLine(String.Format("Paginate: {0} Iterations in {1} seconds, average {2:F2}ms", TOTAL_RUNS, totalElapsed / 1000, totalElapsed / TOTAL_RUNS));
        }
    }
}
