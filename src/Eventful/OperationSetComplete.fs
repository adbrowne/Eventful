namespace Eventful

type OperationSetComplete<'T when 'T : comparison> (operationSet : Set<'T>, callback : Async<unit>) =
    do 
        if(operationSet = Set.empty) then 
            callback 
            |> Async.StartAsTask 
            |> ignore

    let log = createLogger "Eventful.OperationSetComplete"

    let agent = newAgent "OperationSetComplete" log (fun agent ->
        let rec loop state = async {
            let! item = agent.Receive()

            let state' = state |> Set.remove item

            if state'.IsEmpty then
                do! callback
            else
                return! loop state'
        }
         
        loop operationSet
    )

    member this.Complete (item : 'T) =
        agent.Post item