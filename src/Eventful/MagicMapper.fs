namespace Eventful

open System
open System.Reflection

module MagicMapper =
    let magicPropertyGetter<'TId> (objType:Type) : option<obj -> 'TId> =
        let search = 
            objType.GetProperties()
            |> Seq.ofArray
            |> Seq.filter (fun (pi : PropertyInfo) -> pi.PropertyType = typeof<'TId>)
            |> List.ofSeq

        match search with
        | [pi] -> Some (fun o -> pi.GetMethod.Invoke(o, [||])  :?> 'TId)
        | _ -> None

    let magicId<'TId> (item:obj) =
        let objType = item.GetType()
        let getter = magicPropertyGetter<'TId> objType
        match getter with
        | Some getter -> getter item
        | None -> failwith <| sprintf "Unable to find unambiguous property %A on type: %A" typeof<'TId> objType
