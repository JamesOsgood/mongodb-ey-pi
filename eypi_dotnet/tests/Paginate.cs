using System;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Diagnostics;
using System.Threading;

namespace eypi_dotnet.tests
{
    class Paginate : MongoDBTest
    {
        public override void RunTest( string connectionString )
        {
            int TOTAL_RUNS = 10;
            long totalElapsed = 0;
            for ( int i = 0; i < TOTAL_RUNS; i++)
            {
                var sw = Stopwatch.StartNew();
                var db = base.ConnectToMongoDB(connectionString, "eypi");
                var collection = db.GetCollection<BsonDocument>("records");
                var documents = collection.Find(new BsonDocument()).Limit(100).Skip(300).ToList();
                Console.WriteLine(documents.Count);
                sw.Stop();
                totalElapsed += sw.ElapsedMilliseconds;
                Thread.Sleep(500);
            }
        
            Console.Out.WriteLine(String.Format("Paginate: {0} Iterations in {1} seconds, average {2:F2}ms", TOTAL_RUNS, totalElapsed / 1000, totalElapsed / TOTAL_RUNS));
        }
    }
}
