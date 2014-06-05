namespace Eventful

open System
open System.Reflection
open Microsoft.FSharp.Reflection

module MagicMapper =
    let (?) (obj : obj) (nm : string) : 'T =
        obj.GetType().InvokeMember(nm, BindingFlags.GetProperty, null, obj, [||])
        |> unbox<'T>

    let (?<-) (obj : obj) (nm : string) (v : obj) : unit =
        obj.GetType().InvokeMember(nm, BindingFlags.SetProperty, null, obj, [|v|])
        |> ignore

    let magicPropertyGetter<'TId> (objType:Type) : option<obj -> 'TId> =
        let search = 
            objType.GetProperties()
            |> Seq.ofArray
            |> Seq.filter (fun (pi : PropertyInfo) -> pi.PropertyType = typeof<'TId>)
            |> List.ofSeq

        match search with
        | [pi] -> Some (fun o -> pi.GetMethod.Invoke(o, [||])  :?> 'TId)
        | _ -> None

    let magicIdFromType<'TId> theType =
        let getter = magicPropertyGetter<'TId> theType
        match getter with
        | Some getter -> (fun t -> getter t)
        | None -> failwith <| sprintf "Unable to find unambiguous property %A on type: %A" typeof<'TId> theType

    let magicId<'TId> (item:obj) =
        let objType = item.GetType()
        let getter = magicPropertyGetter<'TId> objType
        match getter with
        | Some getter -> getter item
        | None -> failwith <| sprintf "Unable to find unambiguous property %A on type: %A" typeof<'TId> objType

    let getWrapper<'TUnion> () =
        let cases = FSharpType.GetUnionCases(typeof<'TUnion>)
        let wrappableCases = 
            cases
            |> Seq.map (fun case -> (case, case.GetFields()))
            |> Seq.filter (fun (_, fields) -> fields |> Seq.length = 1)
            |> Seq.map (fun (case, fields) -> (case, fields |> Seq.head))
        (fun (value:obj) -> 
            let fieldType = value.GetType()
            let findMatch = 
                wrappableCases 
                |> Seq.filter(fun (case, field) -> field.PropertyType = fieldType)
                |> List.ofSeq

            match findMatch with
            | [(case, field)] -> 
                FSharpValue.MakeUnion(case, [|value|]) :?> 'TUnion
            | _ -> failwith <| sprintf "No unique case for type %A in union type %A" fieldType typeof<'TUnion>
        ) 

    let getUnwrapper<'TUnion> () =
        let cases = FSharpType.GetUnionCases(typeof<'TUnion>)
        let wrappableCases = 
            cases
            |> Seq.map (fun case -> (case, case.GetFields()))
            |> Seq.filter (fun (_, fields) -> fields |> Seq.length = 1)
            |> Seq.map (fun (case, fields) -> (case, fields |> Seq.head))
        (fun (value:'TUnion) -> 
            let valueType = value.GetType()
            if (FSharpType.IsUnion(valueType)) then
                //FSharpType.GetUnionCases(valueType)
                value?Item
            else 
                failwith <| sprintf "Not a union type value %A" (value.GetType())
        ) 