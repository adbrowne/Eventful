namespace Eventful

module FreeMonad =
    type IFunctor<'A> =
        abstract member Fmap : ('A -> 'B) -> IFunctor<'B>
            
    type Toy<'N> =
    | Output of int * 'N
    | GetValue of (int -> 'N)
    | Bell of 'N
    | Done
//        interface IFunctor<'N> with
//            member this.Fmap f = 
//                match this with
//                | Output (x, next) -> Output (x, (f next)) :> IFunctor<_>
//                | Bell next -> Bell (f next) :> IFunctor<_>
//                | Done -> Done :> IFunctor<_>

    let fmap f toy = 
        match toy with
        | Output (x, next) -> Output (x, (f next))
        | GetValue (x) -> GetValue (x >> f)
        | Bell next -> Bell (f next)
        | Done -> Done

    type Free<'F,'R> = 
        | Free of Toy<Free<'F,'R>>
        | Pure of 'R

    let empty = Pure ()

    // liftF :: (Functor f) => f r -> Free f r -- haskell signature
    let liftF command = Free (fmap Pure command)

    let output x = Free (Output(x, Pure ()))
    let getValue<'T> : Free<'T,int> = liftF (GetValue id) // Free (GetValue(Pure 1))
    let bell = Free (Bell (Pure ()))
    let done2 = Free (Done)

    let returnM = Pure
    let rec bind f v =
        match v with
        | Free x -> Free (fmap (bind f) x)
        | Pure r -> f r

    type FreeBuilder<'F>() =
        member x.Zero() = returnM ()
        member x.Return(r:'R) : Free<'F,'R> = returnM r
        member x.ReturnFrom(r:Free<'F,'R>) : Free<'F,'R> = r
        member x.Bind (inp : Free<'F,'R>, body : ('R -> Free<'F,'U>)) : Free<'F,'U>  = bind body inp

    let free<'F> = new FreeBuilder<'F>()

open FreeMonad 
open Xunit

module Play =
    
    let toyProgram<'A> = free<Toy<'A>> {
        do! output 1
        let! value = getValue
        return value
    }

    let rec interpret prog = 
        match prog with
        | Free (GetValue g) -> 
            printfn "GetValue" 
            let next = g 1
            interpret next
        | Free (Output (v, next)) ->
            printfn "Output %A" v
            interpret next
        | Free (Bell next) ->
            printfn "Bell"
            interpret next
        | Pure result ->
            printfn "Pure %A" result
        | Free Done ->
            printfn "Done"

    [<Fact>]
    let ``can run program`` () : unit =
        interpret toyProgram

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