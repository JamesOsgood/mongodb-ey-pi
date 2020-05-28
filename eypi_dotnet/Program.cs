using System;
using CommandLine;
using System.Collections.Generic;
using eypi_dotnet.tests;

namespace eypi_dotnet
{
    class Program
    {
        public class Options
        {
            [Option('u', "uri", Required = true, HelpText = "MongoDB URI")]
            public string Uri { get; set; }

            [Option('c', "command", Required = true, HelpText = "Command to run")]
            public string Command { get; set; }
        }

        static void Main(string[] args)
        {
            Parser.Default.ParseArguments<Options>(args)
                   .WithParsed<Options>(o =>
                   {
                       var runner = new CommandRunner();
                       runner.RunCommand(o.Uri, o.Command);
                   });
        }

        public class CommandRunner
        {
            private Dictionary<string, MongoDBTest> _commands = new Dictionary<string, MongoDBTest>();

            public CommandRunner()
            {
                _commands.Add("paginate", new Paginate());
            }
        
            public void RunCommand(string connectionString, string command )
            {
                if (_commands.ContainsKey(command))
                {
                    var test = _commands[command];
                    test.RunTest(connectionString);
                    Console.Out.WriteLine(String.Format("Succesfully run {0}", command));
                }
                else
                {
                    Console.Error.WriteLine(String.Format("Unknown test {0}", command));
                }
            }
        }
    }
}
