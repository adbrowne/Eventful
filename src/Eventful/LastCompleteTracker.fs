namespace Eventful

type LastCompleteTracker<'TState, 'TMsg> private 
    (
        items : Map<int64, 'TState>, 
        completed: Set<int64>, 
        lastComplete : int64,
        onItemReceived : (('TMsg * 'TState option) -> ('TState option))
    ) =

    let rec itemComplete id lastComplete allComplete =
        let nextInSequence = lastComplete + 1L
        if(id = nextInSequence) then
            if allComplete = Set.empty then
                (id, allComplete)
            else
                let nextComplete = allComplete |> Set.minElement
                itemComplete nextComplete id (allComplete |> Set.remove nextComplete)
        else
            (lastComplete, allComplete |> Set.add id)

    static let empty onItemReceived =
        new LastCompleteTracker<'TState, 'TMsg>(Map.empty, Set.empty, -1L, onItemReceived)

    static member Empty onItemReceived = empty onItemReceived

    member x.LastComplete = lastComplete

    member x.Process (id : int64) operation =
        let state = items |> Map.tryFind id
        let newState = onItemReceived (operation, state)

        match newState with
        | None -> 
            let (lastComplete', completed') = itemComplete id lastComplete completed
            (true, new LastCompleteTracker<_,_>(items |> Map.remove id, completed', lastComplete', onItemReceived)) 
        | Some state -> 
            (false, new LastCompleteTracker<_,_>(items |> Map.add id state, completed, lastComplete, onItemReceived))