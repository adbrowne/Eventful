namespace Eventful

open System

type LastCompleteItemAgent2<'TItem when 'TItem : comparison> () = 
    let log (msg : string) = Console.WriteLine(msg)
    let started = new System.Collections.Generic.SortedSet<'TItem>()
    let completed = new System.Collections.Generic.SortedSet<'TItem>()
    let agent = Agent.Start(fun agent ->

        let rec loop state = async {
            let! msg = agent.Receive()
            match msg with
            | Start (item, reply) ->
                reply.Reply()
                started.Add(item) |> ignore
                return! loop state
            | Complete item ->
                completed.Add(item) |> ignore
                return! loop state
            | LastComplete reply ->
                let lastComplete = 
                    Seq.zip started completed
                    |> Seq.takeWhile (fun (x,y) -> x = y)
                    |> Seq.fold (fun _ (x,y) -> Some x) None
                
                reply.Reply(lastComplete)
                match lastComplete with
                | None ->
                    ()
                | Some lastComplete ->
                    started.RemoveWhere((fun x -> x <= lastComplete)) |> ignore
                    completed.RemoveWhere((fun x -> x <= lastComplete)) |> ignore
                return! loop ()
        }

        loop ()
    )

    member x.LastComplete () : Async<'TItem option> =
        agent.PostAndAsyncReply((fun ch -> LastComplete(ch)))

    member x.Start(item, ?timeout) = 
      agent.PostAndAsyncReply((fun ch -> Start (item,ch)), ?timeout=timeout)

    member x.Complete(item) = 
      agent.Post(Complete item)