namespace Eventful.Tests.Integration

open Xunit

module PerformanceSuiteTests =
    [<Fact>]
    let ``Full Integration Run`` () =
        use eventStoreProcess = InMemoryEventStoreRunner.startInMemoryEventStore()
        use ravenProcess = InMemoryRavenRunner.startNewProcess()

        ()