namespace Eventful

open System
open FSharpx.Collections
open FSharpx

type SortedSet<'a> = System.Collections.Generic.SortedSet<'a>
type LastCompleteItemAgent2<'TItem when 'TItem : comparison> () = 
    let log (msg : string) = Console.WriteLine(msg)
    let started = new System.Collections.Generic.SortedSet<'TItem>()
    let completed = new System.Collections.Generic.SortedSet<'TItem>()
    let mutable currentLastComplete = None

    // remove matching head sequences from xs and ys
    // returns sequences and highest matching value
    let removeMatchingHeads (xs : SortedSet<'TItem>) (ys : SortedSet<'TItem>) =
        let rec loop h = 
            match (xs |> Seq.tryHead, ys |> Seq.tryHead) with
            | Some x, Some y
                when x = y ->
                    xs.Remove(x) |> ignore
                    ys.Remove(y) |> ignore
                    loop (Some x)
            | _ -> h

        loop None

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
                let lastComplete' = removeMatchingHeads started completed

                currentLastComplete <-
                    match lastComplete' with
                    | Some x -> Some x
                    | None -> currentLastComplete

                reply.Reply(currentLastComplete)
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