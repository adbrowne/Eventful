namespace Eventful

module FreeMonad =
    type IFunctor<'A> =
        abstract member Fmap : ('A -> 'B) -> IFunctor<'B>
            
    type Toy<'T,'N> =
    | Output of 'T * 'N
    | Bell of 'N
    | Done
        interface IFunctor<'N> with
            member this.Fmap f = 
                match this with
                | Output (x, next) -> Output (x, (f next)) :> IFunctor<_>
                | Bell next -> Bell (f next) :> IFunctor<_>
                | Done -> Done :> IFunctor<_>

    let fmap f toy = 
        match toy with
        | Output (x, next) -> Output (x, (f next))
        | Bell next -> Bell (f next)
        | Done -> Done

    type Free<'F,'T,'R> = 
        | Free of Toy<'T,Free<'F,'T,'R>>
        | Pure of 'R

    let empty = Pure ()
    let output x = Free (Output(x, Pure ()))
    let bell = Free (Bell (Pure ()))
    let done2 = Free (Done)

    let returnM = Pure
    let rec bind f v =
        match v with
        | Free x -> Free (fmap (bind f) x)
        | Pure r -> f r

    type FreeBuilder<'T,'F>() =
        member x.Return(r:'R) : Free<'F,'T,'R> = returnM r
        member x.ReturnFrom(r:Free<'F,'T,'R>) : Free<'F,'T,'R> = r
        member x.Bind (inp : Free<'F,'T,'R>, body : ('R -> Free<'F,'T,'U>)) : Free<'F,'T,'U>  = bind body inp

    let free<'T,'F> = new FreeBuilder<'T,'F>()

open FreeMonad 

module Play =
    
    let result<'A> = free<char,Toy<char,'A>> {
        do! output 'a'
        // return! done2
    }

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