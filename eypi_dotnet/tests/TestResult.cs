using System;
using MongoDB.Bson.Serialization.Attributes;

namespace eypi_dotnet.tests
{
    class TestResult
    {
        [BsonElement("test_id")]
        public string TestID { get; set; }

        [BsonElement("instance_id")]
        public string InstanceID { get; set; }

        [BsonElement("ts")]
        public DateTime TS { get; set; }

        [BsonElement("time_taken")]
        public double TimeTaken { get; set; }
    }
}