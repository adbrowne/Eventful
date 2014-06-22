namespace Eventful

module FreeMonad =
            
    type Toy<'N> =
    | Output of int * 'N
    | GetValue of (int -> 'N)
    | Bell of 'N
    | Done

    let fmap f toy = 
        match toy with
        | Output (x, next) -> Output (x, (f next))
        | GetValue (x) -> GetValue (x >> f)
        | Bell next -> Bell (f next)
        | Done -> Done

    type Free<'F,'R> = 
        | Free of Toy<Free<'F,'R>>
        | Pure of 'R

    type FreeProgram<'A> = Free<obj,'A>

    let empty = Pure ()

    // liftF :: (Functor f) => f r -> Free f r -- haskell signature
    let liftF command = Free (fmap Pure command)

    let output x = liftF (Output (x,()))
    let getValue<'T> = liftF (GetValue id) // Free (GetValue(Pure 1))
    let bell<'T> = liftF (Bell ())
    let endProgram = Free (Done)

    let rec bind f v =
        match v with
        | Free x -> Free (fmap (bind f) x)
        | Pure r -> f r

    type FreeBuilder() =
        member x.Zero() = Pure ()
        member x.Return(r:'R) : Free<'F,'R> = Pure r
        member x.ReturnFrom(r:Free<'F,'R>) : Free<'F,'R> = r
        member x.Bind (inp : Free<'F,'R>, body : ('R -> Free<'F,'U>)) : Free<'F,'U>  = bind body inp

    let free = new FreeBuilder()

open FreeMonad 
open Xunit
open FsUnit.Xunit

module Play =
    
    let rec interpret prog = 
        match prog with
        | Free (GetValue g) -> 
            let next = g 1
            "GetValue" :: interpret next
        | Free (Output (v, next)) ->
            (sprintf "Output %A" v) :: interpret next
        | Free (Bell next) ->
            "Bell"::interpret next
        | Pure result ->
            [sprintf "Pure %A" result]
        | Free Done ->
            ["Done"]

    [<Fact>]
    let ``can run program`` () : unit =
        let toyProgram : FreeProgram<int> = free {
            do! output 1
            let! value = getValue
            return value
        }

        let result = interpret toyProgram

        result |> should equal ["Output 1"; "GetValue"; "Pure 1"]

//module StreamWorker =
//    type StreamWorkerState<'T> =
//    | Result of 'T
//    | ReadStreamItem of int * string * StreamWorkerState<'T>
//    | WriteStreamItem of int * string * StreamWorkerState<'T>
//
//    let bind
//
//    type StreamWorkerBuilder() =
//        member x.Return(()) = StreamWorkerState.None 
//        member x.Bind (inp:StreamWorkerState<'T>, body : 'T -> StreamWorkerState<'U>) : StreamWorkerState<'U> = 
//          bind(inp, body)
//
//    let streamWorker = StreamWorkerBuilder()