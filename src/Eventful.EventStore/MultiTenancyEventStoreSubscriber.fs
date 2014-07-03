namespace Eventful.EventStore

open EventStore.ClientAPI
open Eventful
open FSharp.Control

type MultiTenancyEventStoreSubscriber<'TWrappedEvent>
    (
        ignoredStreams : seq<string>,
        ignoredEventTypes : seq<string>,
        tenancies : Map<string, string>,
        wrapEvent : ResolvedEvent -> 'TWrappedEvent,
        forceEvent : 'TWrappedEvent -> unit,
        getTenancy : 'TWrappedEvent -> string,
        connection : IEventStoreConnection,
        parallelDeserializers : int,
        maxQueueSize : int
    ) =

    let ignoredStreamsHash = new System.Collections.Generic.HashSet<string>(ignoredStreams)
    let ignoredEventTypesHash = new System.Collections.Generic.HashSet<string>(ignoredEventTypes)

    let client = new Client(connection)

    let buffer = new BlockingQueueAgent<'TWrappedEvent>(maxQueueSize)

    let handle eventGuid (resolvedEvent : ResolvedEvent) = 
        if ignoredStreamsHash.Contains(resolvedEvent.OriginalStreamId) then
            async { () } 
        elif ignoredEventTypesHash.Contains(resolvedEvent.Event.EventType) then
            async { () } 
        else 
            async { 
                do! buffer.AsyncAdd(wrapEvent resolvedEvent)
            }

    member x.Start () =
        let subscription : EventStoreAllCatchUpSubscription = client.subscribe None handle (fun () -> consoleLog "LiveProcessingStarted")

        for i in 0..parallelDeserializers do
            async {
                let rec loop () = async {
                    let! next = buffer.AsyncGet()
                    forceEvent next
                    return! loop ()
                }

                do! loop()
            } |> Async.Start