namespace Eventful.Tests.Integration

open Xunit
open System
open Raven.Json.Linq
open FSharpx.Collections
open FSharpx
open FsUnit.Xunit
open Raven.Client
open Eventful
open Eventful.Raven
open Eventful.Tests

module SnapshotProjectorTests =
    open Eventful.AggregateActionBuilder
    open TestEventStoreSystemHelpers
    open Eventful.Aggregate

    let serializer = RunningTests.esSerializer

    type ProjectorEvent = 
        {
            StreamId : string
            EventNumber : int
            Event: obj
            Metadata: TestMetadata }
        interface IBulkMessage with
            member x.GlobalPosition = None
            member x.EventType = x.Event.GetType()
        static member GetStreamId { ProjectorEvent.StreamId = x } = x
        static member GetEventNumber { ProjectorEvent.EventNumber = x } = x
        static member GetEvent { ProjectorEvent.Event = x } = x
        static member GetMetadata { ProjectorEvent.Metadata = x } = x

    let widgetCountStateBuilder =
        StateBuilder.Empty "WidgetCount" 0
        |> StateBuilder.aggregateStateHandler (fun (s,e:WidgetCreatedEvent,m) -> s + 1)
        |> StateBuilder.toInterface

    let handlers =
        let getStreamName typeName () (id:WidgetId) =
            sprintf "%s-%s" typeName (id.Id.ToString("N"))

        let getEventStreamName typeName (context : MockDisposable) (id:WidgetId) =
            sprintf "%s-%s" typeName (id.Id.ToString("N"))
            
        let widgetCmdHandlers = 
            seq {
                   let addWidget count (cmd : CreateWidgetCommand) =
                       { 
                           WidgetId = cmd.WidgetId
                           Name = cmd.Name } 

                   yield addWidget
                         |> cmdBuilderS widgetCountStateBuilder
                         |> buildCmd
                }

        let widgetHandlers = 
            toAggregateDefinition 
                "Widget" 
                TestMetadata.GetUniqueId
                (getStreamName "Widget") 
                (getEventStreamName "Widget") 
                widgetCmdHandlers 
                Seq.empty

        EventfulHandlers.empty TestMetadata.GetAggregateType
        |> EventfulHandlers.addAggregate widgetHandlers
        |> StandardConventions.addEventType typeof<WidgetCreatedEvent>

    let projectors = 
        AggregateStateProjector.buildProjector 
            ProjectorEvent.GetStreamId
            ProjectorEvent.GetEventNumber
            ProjectorEvent.GetEvent
            ProjectorEvent.GetMetadata
            serializer
            handlers
        |> Seq.singleton

    let documentStore = RavenProjectorTests.buildDocumentStore ()

    let widgetId = { WidgetId.Id = Guid.NewGuid() }

    let streamId = sprintf "Widget-%s" (widgetId.Id.ToString("N"))
    let stateDocumentKey = AggregateStateProjector.getDocumentKey streamId


    let runEvents events = async {
        let ravenProjector = RavenProjectorTests.buildRavenProjector documentStore projectors (fun _ -> Async.returnM ())
        ravenProjector.StartWork()

        use dbCommands = documentStore.AsyncDatabaseCommands.ForDatabase(RavenProjectorTests.testDatabase)

        do! dbCommands.DeleteDocumentAsync stateDocumentKey 
            |> Async.AwaitIAsyncResult
            |> Async.Ignore

        for event in events do
            do! ravenProjector.Enqueue event

        do! ravenProjector.WaitAll()
    }

    let testMetadata = 
        {
            TestMetadata.AggregateType = "Widget"
            SourceMessageId = "ignored"
        } 

    let getSnapshotData = async {
        use session = documentStore.OpenAsyncSession(RavenProjectorTests.testDatabase)
        let! snapshotDoc = session.LoadAsync<RavenJObject> stateDocumentKey |> Async.AwaitTask
        let blockBuilders = (handlers.AggregateTypes.Item "Widget").StateBuilder.GetBlockBuilders
        return AggregateStateProjector.deserialize serializer snapshotDoc blockBuilders
    }
        
    [<Fact>]
    [<Trait("category", "ravendb")>]
    let ``Given no events When first event happens Then snapshot is created`` () =
        async {
            let events = 
                {
                    StreamId = streamId
                    EventNumber = 0
                    Event = 
                        {
                            WidgetCreatedEvent.WidgetId = widgetId
                            Name = "Widget 1"
                        }
                    Metadata = testMetadata
                }
                |> Seq.singleton

            do! runEvents events

            let! snapshotData = getSnapshotData
            widgetCountStateBuilder.GetState snapshotData |> should equal 1
        }
        |> Async.RunSynchronously
        ()

    [<Fact>]
    [<Trait("category", "ravendb")>]
    let ``Given existing events When event happens Then snapshot is updated`` () =
        async {
            let events =  seq {
                yield {
                    StreamId = streamId
                    EventNumber = 0
                    Event = 
                        {
                            WidgetCreatedEvent.WidgetId = widgetId
                            Name = "Widget 1"
                        }
                    Metadata = testMetadata
                }

                yield {
                    StreamId = streamId
                    EventNumber = 1
                    Event = 
                        {
                            WidgetCreatedEvent.WidgetId = widgetId
                            Name = "Widget 2"
                        }
                    Metadata = testMetadata
                }
            }

            do! runEvents events

            let! snapshotData = getSnapshotData
            widgetCountStateBuilder.GetState snapshotData |> should equal 2
        }
        |> Async.RunSynchronously
        ()
