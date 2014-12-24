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
open System.ComponentModel
open FSharp.Control
open Swensen.Unquote

module SnapshotProjectorTests =
    open Eventful.AggregateActionBuilder
    open TestEventStoreSystemHelpers
    open Eventful.Aggregate

    [<System.ComponentModel.TypeConverter(typeof<SnappyIdTypeConverter>)>]
    type SnappyId = 
        { Id : Guid } 
        override x.ToString() = x.Id.ToString()
    and SnappyIdTypeConverter() =
        inherit System.ComponentModel.TypeConverter()

        override x.CanConvertFrom (context : ITypeDescriptorContext , sourceType : Type) =
            sourceType = typeof<string> || (x :> TypeConverter).CanConvertFrom(context, sourceType)

        override x.ConvertFrom (context : ITypeDescriptorContext , culture : System.Globalization.CultureInfo, value : obj) =
            match value with
            | :? string as value ->
                 let guid = new Guid(value.Trim());
                 { SnappyId.Id = guid } :> obj
            | _ ->
                (x :> TypeConverter).ConvertFrom(context, culture, value)

    type SnappyCreatedEvent = {
        SnappyId : SnappyId
        Name : string
        Notify : DateTime
    }

    type SnappyNotificationSent = {
        SnappyId : SnappyId
    }

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

    let snappyCountStateBuilder =
        StateBuilder.Empty "SnappyCount" 0
        |> StateBuilder.aggregateStateHandler (fun (s,e:SnappyCreatedEvent,m) -> s + 1)
        |> StateBuilder.toInterface

    let toPendingNotificationsStateBuilder = 
        StateBuilder.Empty "PendingNotifications" Map.empty
        |> StateBuilder.aggregateStateHandler (fun (s, e:SnappyCreatedEvent,m) -> s |> Map.add e.SnappyId e.Notify)
        |> StateBuilder.aggregateStateHandler (fun (s, e:SnappyNotificationSent,m) -> s |> Map.remove e.SnappyId)

    let nextWakeupStateBuilder = 
        toPendingNotificationsStateBuilder 
        |> AggregateStateBuilder.map (fun s -> s |> Map.values |> Seq.sort |> Seq.tryHead)
        |> AggregateStateBuilder.map (Option.map UtcDateTime.fromDateTime)

    let handlers =
        let getStreamName typeName () (id:SnappyId) =
            sprintf "%s-%s" typeName (id.Id.ToString("N"))

        let getEventStreamName typeName (context : MockDisposable) (id:SnappyId) =
            sprintf "%s-%s" typeName (id.Id.ToString("N"))
            
        let snappyCmdHandlers = 
            seq {
                   let addSnappy count (cmd : CreateWidgetCommand) =
                       Seq.empty

                   yield addSnappy
                         |> cmdBuilderS snappyCountStateBuilder
                         |> buildCmd
                }

        let snappyHandlers = 
            toAggregateDefinition 
                "Snappy" 
                TestMetadata.GetUniqueId
                (getStreamName "Widget") 
                (getEventStreamName "Widget") 
                snappyCmdHandlers 
                Seq.empty
            |> withWakeup 
                nextWakeupStateBuilder  
                toPendingNotificationsStateBuilder
                (fun t p -> 
                    p 
                    |> Map.filter (fun k v -> v |> UtcDateTime.fromDateTime = t)
                    |> Map.keys
                    |> Seq.map (fun x -> ({ SnappyNotificationSent.SnappyId = x } :> obj, buildMetadata))
                )

        EventfulHandlers.empty TestMetadata.GetAggregateType
        |> EventfulHandlers.addAggregate snappyHandlers
        |> StandardConventions.addEventType typeof<SnappyCreatedEvent>
        |> StandardConventions.addEventType typeof<SnappyNotificationSent>

    let buildPersistedEvent projectorEvent =
        {
            PersistedEvent.StreamId = ProjectorEvent.GetStreamId projectorEvent
            EventNumber = ProjectorEvent.GetEventNumber projectorEvent
            EventId = (Guid.NewGuid())
            Body = ProjectorEvent.GetEvent projectorEvent
            EventType = "Ignored"
            Metadata = ProjectorEvent.GetMetadata projectorEvent
        }
        |> Some

    let projectors = 
        AggregateStatePersistence.buildProjector 
            buildPersistedEvent
            serializer
            handlers
        :> IProjector<_,_,_>
        |> Seq.singleton

    let documentStore = RavenProjectorTests.buildDocumentStore ()

    let snappyId = { SnappyId.Id = Guid.NewGuid() }

    let streamId = sprintf "Snappy-%s" (snappyId.Id.ToString("N"))
    let stateDocumentKey = AggregateStatePersistence.getDocumentKey streamId

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

    let aggregateType = "Snappy"

    let testMetadata = 
        {
            TestMetadata.AggregateType = aggregateType
            SourceMessageId = "ignored"
        } 

    let stateBuilder = (handlers.AggregateTypes.Item aggregateType).StateBuilder

    let getAggregateState = 
        AggregateStatePersistence.getAggregateState
            documentStore
            serializer
            RavenProjectorTests.testDatabase
            streamId
            (stateBuilder |> StateBuilder.getTypeMapFromStateBuilder)

    let getSnapshotData = 
        getAggregateState 
        |> Async.map (fun x -> x.Snapshot)
        
    let buildEvent notify = {
        StreamId = streamId
        EventNumber = 0
        Event = 
            {
                SnappyCreatedEvent.SnappyId = snappyId
                Name = "Snappy 1"
                Notify = notify
            }
        Metadata = testMetadata
    }

    let event = buildEvent DateTime.MinValue
        
    [<Fact>]
    [<Trait("category", "ravendb")>]
    let ``Install Wakeup Index`` () =
        let definition = AggregateStatePersistence.wakeupIndex()
        documentStore.DatabaseCommands.ForDatabase(RavenProjectorTests.testDatabase).PutIndex(definition.Name, definition, true) |> ignore
        ()

    [<Fact>]
    [<Trait("category", "ravendb")>]
    let ``Given no events When first event happens Then snapshot is created`` () =
        async {
            let events = 
                event |> Seq.singleton

            do! runEvents events

            let! snapshotData = getSnapshotData
            snappyCountStateBuilder.GetState snapshotData.State |> should equal 1
        }
        |> Async.RunSynchronously
        ()

    [<Fact>]
    [<Trait("category", "ravendb")>]
    let ``Given existing events When event happens Then snapshot is updated`` () =
        async {
            let events =  seq {
                yield { event with EventNumber = 0 }
                yield { event with EventNumber = 1 }
            }

            do! runEvents events

            let! snapshotData = getSnapshotData
            snappyCountStateBuilder.GetState snapshotData.State |> should equal 2
        }
        |> Async.RunSynchronously
        ()

    [<Fact>]
    [<Trait("category", "ravendb")>]
    let ``Given two identical events Then second one is ignored`` () =
        async {
            let events =  seq {
                yield event
                yield event
            }

            do! runEvents events

            let! snapshotData = getSnapshotData
            snappyCountStateBuilder.GetState snapshotData.State |> should equal 1
        }
        |> Async.RunSynchronously
        ()

    [<Fact>]
    [<Trait("category", "ravendb")>]
    let ``StreamId is stored`` () =
        async {
            let notifyTime = DateTime.Parse("1 January 2009")
            let events = 
                buildEvent notifyTime |> Seq.singleton

            do! runEvents events

            use dbCommands = documentStore.AsyncDatabaseCommands.ForDatabase(RavenProjectorTests.testDatabase)

            let wakeupStreams = 
                WakeupMonitorModule.getWakeups true dbCommands UtcDateTime.maxValue
                |> AsyncSeq.map (fun (streamId,_,_) -> streamId)
                |> AsyncSeq.toBlockingSeq
                |> List.ofSeq

            test <@ wakeupStreams |> List.exists (fun s -> s = streamId) @>
        }
        |> Async.RunSynchronously
        ()

    [<Fact>]
    [<Trait("category", "ravendb")>]
    let ``AggregateType is stored`` () =
        async {
            let notifyTime = DateTime.Parse("1 January 2009")
            let events = 
                buildEvent notifyTime |> Seq.singleton

            do! runEvents events

            use dbCommands = documentStore.AsyncDatabaseCommands.ForDatabase(RavenProjectorTests.testDatabase)

            WakeupMonitorModule.getWakeups true dbCommands UtcDateTime.maxValue
            |> AsyncSeq.filter (fun (s,_,_) -> s = streamId)
            |> AsyncSeq.map (fun (_,aggregateType,_) -> aggregateType)
            |> AsyncSeq.toBlockingSeq
            |> Seq.head
            =? aggregateType 
        }
        |> Async.RunSynchronously
        ()

    [<Fact>]
    [<Trait("category", "ravendb")>]
    let ``WakeupTime is returned`` () =
        async {
            let notifyTime = DateTime.Parse("2008-01-01 00:00:11 GMT")
            let events = 
                buildEvent notifyTime |> Seq.singleton

            do! runEvents events

            use dbCommands = documentStore.AsyncDatabaseCommands.ForDatabase(RavenProjectorTests.testDatabase)

            let wakeupStreams = 
                WakeupMonitorModule.getWakeups true dbCommands (notifyTime |> UtcDateTime.fromDateTime)
                |> AsyncSeq.map (fun (_,_,wakeupTime) -> wakeupTime)
                |> AsyncSeq.toBlockingSeq
                |> List.ofSeq

            let utcNotifyTime = (notifyTime |> UtcDateTime.fromDateTime)
            test <@ wakeupStreams |> List.exists (fun t -> t = utcNotifyTime) @>
        }
        |> Async.RunSynchronously
        ()

    [<Fact>]
    [<Trait("category", "ravendb")>]
    let ``Next wakeup is computed`` () =
        async {
            let notifyTime = DateTime.Parse("2008-01-01 00:00:11 GMT")
            let events = 
                buildEvent notifyTime |> Seq.singleton

            do! runEvents events

            let! aggregateState = getAggregateState
            aggregateState.NextWakeup |> should equal (Some notifyTime)
        }
        |> Async.RunSynchronously
        ()