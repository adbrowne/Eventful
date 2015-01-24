﻿namespace Eventful.Tests.Integration

open Xunit
open System
open System.Threading.Tasks
open FsUnit.Xunit
open Eventful
open Eventful.EventStream
open Eventful.EventStore
open Eventful.Testing
open FSharpx.Collections
open Eventful.Tests
open FSharpx
open Swensen.Unquote

type MyEvent = {
    Name : string
}

type NumberValue = {
    Value : int
}

type EventStoreStreamInterpreterTests () = 

    let mutable connection : EventStore.ClientAPI.IEventStoreConnection = null

    let newId () : string =
        Guid.NewGuid().ToString()

    let event = { Name = "Andrew Browne" }
    let metadata = { 
        TestMetadata.SourceMessageId = None
        AggregateType = "TestAggregate" }

    let classToEventTypeName =
        PersistentHashMap.empty
        |> PersistentHashMap.add typeof<MyEvent> typeof<MyEvent>.Name
        |> PersistentHashMap.add typeof<NumberValue> typeof<NumberValue>.Name

    let eventTypeToClassMap = 
        PersistentHashMap.empty 
        |> PersistentHashMap.add typeof<MyEvent>.Name typeof<MyEvent> 
        |> PersistentHashMap.add typeof<NumberValue>.Name typeof<NumberValue> 

    let inMemoryCache = new System.Runtime.Caching.MemoryCache("EventfulEvents")

    let nullGetSnapshot streamId typeMap = StateSnapshot.Empty |> Async.returnM

    let run client program =
        EventStreamInterpreter.interpret 
            client 
            inMemoryCache 
            RunningTests.esSerializer 
            eventTypeToClassMap 
            classToEventTypeName 
            nullGetSnapshot
            {
                CorrelationId = Guid.NewGuid() |> Some
                ContextId = Guid.NewGuid()
                Name = "Interpreter test"
                Stopwatch = startStopwatch()
                ExtraTemplate = String.Empty
                ExtraVariables = Array.empty
            }
            program

    [<Fact>]
    [<Trait("category", "eventstore")>]
    let ``Write and read Event`` () : unit =
        async {
            let client = new EventStoreClient(connection)

            let run program = run client program

            let stream = "MyStream-" + (newId())

            let! writeResult = 
                eventStream {
                    let! eventToWrite = getEventStreamEvent event metadata
                    let writes : seq<EventStreamEvent<TestMetadata>> = Seq.singleton eventToWrite
                    let! ignore = writeToStream stream NewStream writes
                    return "Write Complete"
                } |> run

            writeResult |> should equal "Write Complete"

            let! readResult =
                eventStream {
                    let! item = readFromStream stream EventStore.ClientAPI.StreamPosition.Start
                    return!
                        match item with
                        | Some x -> 
                            eventStream { 
                                let! (objValue, _) = readValue x
                                let value = objValue :?> MyEvent
                                return Some value.Name
                            }
                        | None -> eventStream { return None } 
                } |> run

            readResult |> should equal (Some "Andrew Browne")

        } |> Async.RunSynchronously

    [<Fact>]
    [<Trait("category", "eventstore")>]
    let ``Write and read sequence`` () : unit =
        async {
            let client = new EventStoreClient(connection)

            let run program = run client program

            let stream = "MyStream-" + (newId())

            let rec fibs a b = seq {
                let c = a + b
                yield c
                yield! fibs b c
            }

            let valueSequence = 
                Seq.init 1000 id
                |> Seq.cache
                |> List.ofSeq
                
            let! writeResult = 
                eventStream {
                    let! writes = 
                        valueSequence
                        |> EventStream.mapM (fun i -> getEventStreamEvent { NumberValue.Value = i } metadata)
                    let! ignore = writeToStream stream NewStream writes
                    return "Write Complete"
                } |> run

            writeResult |> should equal "Write Complete"

            let acc s (objValue : obj, _) =
                let value = objValue :?> NumberValue
                s + value.Value

            let! readResult = EventStream.foldStream stream EventStore.ClientAPI.StreamPosition.Start acc 0 |> run

            let expectedResult = valueSequence |> Seq.sum

            readResult |> should equal expectedResult

        } |> Async.RunSynchronously

    [<Fact>]
    [<Trait("category", "eventstore")>]
    let ``Wrong Expected Version is Returned`` () : unit =
        async {
            let client = new EventStoreClient(connection)

            let run program = run client program

            let stream = "MyStream-" + (newId())

            let wrongExpectedVersion = AggregateVersion 10
            let! writeResult = 
                eventStream {
                    let! eventToWrite = getEventStreamEvent event metadata
                    let writes : seq<EventStreamEvent<TestMetadata>> = Seq.singleton eventToWrite
                    return! writeToStream stream wrongExpectedVersion writes
                } |> run

            writeResult |> should equal WriteResult.WrongExpectedVersion
        } |> Async.RunSynchronously

    [<Fact>]
    [<Trait("category", "eventstore")>]
    let ``Write Position is returned`` () : unit =
        async {
            let client = new EventStoreClient(connection)

            let run program = run client program

            let stream = "MyStream-" + (newId())

            let! writeResult = 
                eventStream {
                    let! eventToWrite = getEventStreamEvent event metadata
                    let writes : seq<EventStreamEvent<TestMetadata>> = Seq.singleton eventToWrite
                    let! result = writeToStream stream NewStream writes
                    return result
                } |> run

            match writeResult with
            | WriteResult.WriteSuccess position ->
                let! readResult = client.readEventFromPosition position
                match readResult with
                | None -> failwith "Could not read event back using global position"
                | Some evt ->
                    evt.Event.EventType |> should equal "MyEvent"
            | x -> failwith <| sprintf "Write did not succeed %A" x
        } |> Async.RunSynchronously

    [<Fact>]
    [<Trait("category", "eventstore")>]
    let ``Create a link`` () : unit =
        async {
            let client = new EventStoreClient(connection)

            let run program = run client program

            let sourceStream = "SourceStream-" + (newId())
            let stream = "MyStream-" + (newId())

            let! writeLink = 
                eventStream {
                    let! eventToWrite = getEventStreamEvent event metadata
                    let writes = Seq.singleton eventToWrite
                    let! ignore = writeToStream sourceStream NewStream writes
                    let! ignore = EventStream.writeLink stream NewStream sourceStream 0 metadata
                    return ()
                } |> run

            let! readResult =
                eventStream {
                    let! item = readFromStream stream EventStore.ClientAPI.StreamPosition.Start
                    return!
                        match item with
                        | Some x -> 
                            eventStream { 
                                let! (objValue,_) = readValue x
                                let value = objValue :?> MyEvent
                                return Some value.Name
                            }
                        | None -> eventStream { return None } 
                } |> run

            readResult |> should equal (Some event.Name)
        } |> Async.RunSynchronously


    [<Fact>]
    [<Trait("category", "eventstore")>]
    let ``Set MaxCount in stream metadata`` () : Task<unit> =
        async {
            let client = new EventStoreClient(connection)

            let run program = run client program

            let stream = "MyStream-" + (newId())

            do! run <| writeStreamMetadata stream { EventStreamMetadata.Default with MaxCount = Some 1 }

            let! writtenMetadata = client.getStreamMetadata stream

            let expectedMaxCount = Nullable(1)
            writtenMetadata.StreamMetadata.MaxCount =? Nullable(1)
        } |> Async.StartAsTask

    [<Fact>]
    [<Trait("category", "eventstore")>]
    let ``Set MaxAge in stream metadata`` () : Task<unit> =
        async {
            let client = new EventStoreClient(connection)

            let run program = run client program

            let stream = "MyStream-" + (newId())

            do! run <| writeStreamMetadata stream { EventStreamMetadata.Default with MaxAge = Some (TimeSpan.FromDays(1.)) }

            let! writtenMetadata = client.getStreamMetadata stream

            writtenMetadata.StreamMetadata.MaxAge =? Nullable(TimeSpan.FromDays(1.))
        } |> Async.StartAsTask

    interface Xunit.IUseFixture<EventStoreFixture> with
        member x.SetFixture(fixture) =
            connection <- fixture.Connection