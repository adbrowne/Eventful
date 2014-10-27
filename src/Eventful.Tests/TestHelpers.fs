namespace Eventful.Testing

open Eventful

open FsCheck
open FSharpx.Collections

module TestHelpers = 
    let (|Equals|_|) expected value =
       if expected = value
       then Some ()
       else None

    let beSuccessWithEvent<'A> (x:'A -> bool) = 
        let matches (event : obj) =
            match event with
            | :? 'A as event ->
                x event
            | _ -> false
        NHamcrest.CustomMatcher<obj>(
            sprintf "Matches %A" x, 
            (fun a ->
                match a with
                | :? Choice<list<'A>,NonEmptyList<ValidationFailure>>  as result -> 
                    match result with
                    | Choice1Of2 (events) ->
                        events |> List.exists matches
                    | _ -> false
                | _ -> false)
        )

    let containException<'TMetadata> (context, exnMatcher) = 
        let matches failure = 
            match failure with
            | CommandException (c, exn) ->
                c = context && exnMatcher(exn)
            | _ -> false
        NHamcrest.CustomMatcher<obj>(
            sprintf "Matches %A with exception" context, 
            (fun a ->
                match a with
                | :? CommandResult<'TMetadata> as result -> 
                    match result with
                    | Choice2Of2 errors ->
                        errors |> NonEmptyList.toSeq |> Seq.exists matches
                    | _ -> false
                | _ -> false)
        )

    let containError<'TMetadata> x = 
        let matches msg = msg = x
        NHamcrest.CustomMatcher<obj>(
            sprintf "Matches %A" x, 
            (fun a ->
                match a with
                | :? CommandResult<'TMetadata> as result -> 
                    match result with
                    | Choice2Of2 errors ->
                        errors |> NonEmptyList.toSeq |> Seq.exists matches
                    | _ -> false
                | _ -> false)
        )

    let toObjArb<'a> (arb : Arbitrary<'a> ) : Arbitrary<obj> =
        Arb.convert (fun x -> x :> obj) (fun x -> x :?> 'a) arb

    let setField o fieldName value =
        let field = o.GetType().GetField(fieldName, System.Reflection.BindingFlags.NonPublic ||| System.Reflection.BindingFlags.Instance)
        field.SetValue(o, value)

    let getArbVisitor = {
         new IRegistrationVisitor<unit,Arbitrary<obj>> with
                member x.Visit<'TCmd> t = Arb.from<'TCmd> |> toObjArb
         }

    let getCommandsForAggregate (aggregateDefinition: AggregateDefinition<_,_,_,_>) (fieldName, fieldValue) : Arbitrary<obj seq> =
        let commandTypes : seq<Arbitrary<obj>> =
            aggregateDefinition.Handlers.CommandHandlers
            |> Seq.map (fun x -> x.Visitable.Receive () getArbVisitor)

        let rec genCommandList (xs : list<obj>) length = gen {
            match length with
            | 0 -> return xs |> List.toSeq
            | _ ->
                let! typeArb = Gen.elements commandTypes
                let! nextCmd = typeArb.Generator
                setField nextCmd (sprintf "%s@" fieldName) fieldValue
                return! genCommandList (nextCmd::xs) (length - 1)
        }

        let gen = Gen.sized <| genCommandList []
        let shrink (a:seq<obj>) = 
            let length = a |> Seq.length
            if length = 0 then
                Seq.empty
            else 
                seq {
                    yield a |> Seq.skip 1
                }

        Arb.fromGenShrink (gen, shrink)

    let assertTrueWithin maxWaitMilliseconds error f =
        let sleepTime = 5

        let rec loop remaining =
            if remaining < 0 then
                Xunit.Assert.True(f (), error)
                ()
            else
                if f() then
                    ()
                else
                    System.Threading.Thread.Sleep(sleepTime)
                    loop (remaining - sleepTime)

        loop maxWaitMilliseconds