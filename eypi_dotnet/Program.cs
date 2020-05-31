using System;
using CommandLine;
using System.Collections.Generic;
using eypi_dotnet.tests;
using Newtonsoft.Json;

namespace eypi_dotnet
{
    class Program
    {
        public class Options
        {
            [Option('u', "uri", Required = true, HelpText = "MongoDB URI")]
            public string Uri { get; set; }

            [Option('t', "test_name", Required = true, HelpText = "Test to run")]
            public string TestName { get; set; }

            [Option('a', "test_args", Required = true, HelpText = "Test args")]
            public string TestArgs { get; set; }
        }

        static void Main(string[] args)
        {
            Parser.Default.ParseArguments<Options>(args)
                   .WithParsed<Options>(o =>
                   {
                        Dictionary<string, object> testArgs = 
                        JsonConvert.DeserializeObject<Dictionary<string, object>>(o.TestArgs);
                        var runner = new TestRunner();
                        runner.RunTest(o.Uri, o.TestName, testArgs);
                   });
        }

        public class TestRunner
        {
            private Dictionary<string, MongoDBTest> _tests = new Dictionary<string, MongoDBTest>();

            public TestRunner()
            {
                _tests.Add("paginate", new Paginate());
                _tests.Add("update", new Update());
            }
        
            public void RunTest(string connectionString, string testName, Dictionary<string, object> testArgs )
            {
                if (_tests.ContainsKey(testName))
                {
                    var test = _tests[testName];
                    test.RunTest(connectionString, testArgs);
                    Console.Out.WriteLine(String.Format("Successfully run {0}", testName));
                }
                else
                {
                    Console.Error.WriteLine(String.Format("Unknown test {0}", testName));
                }
            }
        }
    }
}
