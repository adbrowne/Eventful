namespace Eventful

type LastCompleteNode<'T> = 
| End
| Node of int64 * bool * 'T * LastCompleteNode<'T>

type LastCompleteZipper<'T when 'T : comparison> = {
    left : LastCompleteNode<'T>
    right : LastCompleteNode<'T>
    counter : int64
    lastCompleteItem : 'T option
    itemIndex : Map<'T,int64>
} with static member Empty() : LastCompleteZipper<'T> = { left = End; right = End; lastCompleteItem = None; counter = 0L; itemIndex = Map.empty }

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module LastCompleteZipper =
    let empty<'T when 'T : comparison> = LastCompleteZipper<'T>.Empty()

    let moveLeft zipper =
        match zipper with
        | { left = Node (leftIndex, leftComplete, leftItem, leftNext) } ->
            { zipper with
                right = Node(leftIndex, leftComplete, leftItem, zipper.right)
                left = leftNext }
        | { left = End } ->
            failwith "Moved too far left"

    let moveRight zipper =
        match zipper with
        | { right = Node (rightIndex, rightComplete, rightItem, rightNext) } ->
            { zipper with
                left = Node(rightIndex, rightComplete, rightItem, zipper.left)
                right = rightNext }
        | { right = End } ->
            failwith "Moved too far right"

    let rec start<'TItem when 'TItem : comparison> (item : 'TItem) zipper =  
        match zipper with
        | { right = End } -> 
            { zipper with
                right = Node (zipper.counter, false, item, End) 
                itemIndex = zipper.itemIndex |> Map.add item zipper.counter
                counter = zipper.counter + 1L}
        | _ -> start item (moveRight zipper)
            
    let rec focusOnIndex index zipper = 
        match zipper with
        | { right = Node(rightIndex, rightComplete, rightItem, rightNext) } when rightIndex = index -> 
            zipper
        | { right = Node(rightIndex,_,_,_) } when rightIndex > index -> moveLeft zipper |> focusOnIndex index
        | { right = Node(rightIndex,_,_,_) } when rightIndex < index -> moveRight zipper |> focusOnIndex index
        | _ -> failwith "Index not found %d" index

    let markRightNodeComplete zipper = 
        match zipper with
        | { right = Node(rightIndex, _, rightItem, rightNext) } -> 
            { zipper with right = Node (rightIndex, true, rightItem, rightNext) }
        | { right = End } -> failwith "Right node is the end of the zipper"
        
    let rec removeCompleteItems zipper =
        match zipper with
        | { left = End; right = Node(rightIndex, true, rightItem, rightNext) } ->
            { zipper with
                left = End
                right = rightNext
                lastCompleteItem = Some rightItem }
            |> removeCompleteItems
        | _ -> zipper

    let complete<'TItem when 'TItem : comparison> (item : 'TItem) (zipper : LastCompleteZipper<'TItem>) = 
        let itemIndex = zipper.itemIndex |> Map.find item
        zipper
        |> focusOnIndex itemIndex
        |> markRightNodeComplete
        |> removeCompleteItems
                

//    member x.Complete (item:'TItem) =
//        let itemIndex = items |> Map.findKey (fun k v -> v = item)
//        let completed' = completed |> Set.add itemIndex
//        new LastCompleteItemTracker<'TItem>(items, completed', lastCompleteItem, lastStartedIndex, lastCompletedIndex)
//
//    member x.UpdateLastCompleted () =
//        let lastCompletedIndex' = getMinimumCompleted completed lastCompletedIndex
//        let lastComplete' =
//            if lastCompletedIndex' > -1L then
//               items |> Map.find lastCompletedIndex' |> Some
//            else
//                None
//        let items' = items |> Map.filter (fun k v -> k >= lastCompletedIndex')
//        let completed' = completed |> Set.filter (fun v -> v > lastCompletedIndex')
//        new LastCompleteItemTracker<'TItem>(items', completed', lastComplete', lastStartedIndex, lastCompletedIndex')
