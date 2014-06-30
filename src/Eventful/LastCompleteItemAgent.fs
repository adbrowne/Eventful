namespace Eventful

open System

type LastCompleteItemMessage<'TItem when 'TItem : comparison> = 
|    Start of ('TItem * AsyncReplyChannel<unit>) 
|    Complete of 'TItem
|    LastComplete of (AsyncReplyChannel<'TItem option>) 

type LastCompleteItemAgent<'TItem when 'TItem : comparison> () = 
    let log (msg : string) = Console.WriteLine(msg)
    let mutable lastComplete = None
    let agent = Agent.Start(fun agent ->

        let rec loop state = async {
            let! msg = agent.Receive()
            match msg with
            | Start (item, reply) ->
                reply.Reply()
                let state' = state |> LastCompleteZipper.start item
                return! loop state'
            | Complete item ->
                let state' = state |> LastCompleteZipper.complete item
                lastComplete <- state'.lastCompleteItem
                return! loop state'
            | LastComplete reply ->
                reply.Reply(state.lastCompleteItem)
                return! loop state
        }

        loop LastCompleteZipper.empty<'TItem>
    )

    member x.LastComplete () = 
        agent.PostAndAsyncReply((fun ch -> LastComplete(ch)))

    member x.Start(item, ?timeout) = 
      agent.PostAndAsyncReply((fun ch -> Start (item,ch)), ?timeout=timeout)

    member x.Complete(item) = 
      agent.Post(Complete item)