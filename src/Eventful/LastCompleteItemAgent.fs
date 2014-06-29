namespace Eventful

open System

type LastCompleteItemMessage<'TItem> = 
|    Start of ('TItem * AsyncReplyChannel<unit>) 
|    Complete of 'TItem
|    LastComplete of (AsyncReplyChannel<'TItem option>) 

type LastCompleteItemAgent<'TItem when 'TItem : equality> () = 
    let log (msg : string) = Console.WriteLine(msg)
    let mutable lastComplete = None
    let agent = Agent.Start(fun agent ->

        let rec loop state = async {
            let! msg = agent.Receive()
            match msg with
            | Start (item, reply) ->
                reply.Reply()
                let state' = state |> LastCompleteItemTracker.start item
                return! loop state'
            | Complete item ->
                let state' = state |> LastCompleteItemTracker.complete item
                lastComplete <- state'.LastComplete
                return! loop state'
            | LastComplete reply ->
                let state' = state.UpdateLastCompleted()
                reply.Reply(state'.LastComplete)
                return! loop state'
        }

        loop LastCompleteItemTracker<'TItem>.Empty
    )

    member x.LastComplete () : Async<'TItem option> =
        agent.PostAndAsyncReply((fun ch -> LastComplete(ch)))

    member x.Start(item, ?timeout) = 
      agent.PostAndAsyncReply((fun ch -> Start (item,ch)), ?timeout=timeout)

    member x.Complete(item) = 
      agent.Post(Complete item)