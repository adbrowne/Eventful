namespace Eventful.Tests.Integration

open Eventful
open Serilog
open Xunit

module PerformanceSuiteTests =

    [<Fact>]
    let ``Full Integration Run`` () =

        let log = new LoggerConfiguration()
        let log = log
                    .WriteTo.Seq("http://localhost:5341")
                    .WriteTo.ColoredConsole()
                    .MinimumLevel.Debug()
                    .CreateLogger()

        EventfulLog.SetLog log
        use eventStoreProcess = InMemoryEventStoreRunner.startInMemoryEventStore()
        use ravenProcess = InMemoryRavenRunner.startNewProcess()

        let config = { 
            BookLibrary.ApplicationConfig.default_application_config with 
                Raven = { BookLibrary.ApplicationConfig.default_raven_config with Port = ravenProcess.HttpPort }
                EventStore = { BookLibrary.ApplicationConfig.default_eventstore_config with TcpPort = eventStoreProcess.TcpPort }}
        use bookLibraryProcess = BookLibraryRunner.startNewProcess config
        ()