namespace Eventful

open System

type LastCompleteItemMessage<'TItem> = 
|    Start of ('TItem * AsyncReplyChannel<unit>) 
|    Complete of 'TItem

type LastCompleteItemAgent<'TItem when 'TItem : equality> () = 
    let log (msg : string) = Console.WriteLine(msg)
    let mutable lastComplete = None
    let agent = Agent.Start(fun agent ->

        let rec loop state = async {
            let! msg = agent.Receive()
            match msg with
            | Start (item, reply) ->
                log <| sprintf "Started %A" item
                reply.Reply()
                let state' = state |> LastCompleteItemTracker.start item
                return! loop state'
            | Complete item ->
                log <| sprintf "Completed %A" item
                let state' = state |> LastCompleteItemTracker.complete item
                lastComplete <- state'.LastComplete
                return! loop state'
        }

        loop LastCompleteItemTracker<'TItem>.Empty
    )

    member x.LastComplete () : 'TItem option =
        lastComplete

    member x.Start(item, ?timeout) = 
      agent.PostAndAsyncReply((fun ch -> Start (item,ch)), ?timeout=timeout)

    member x.Complete(item) = 
      agent.Post(Complete item)