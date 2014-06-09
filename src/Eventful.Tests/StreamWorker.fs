namespace Eventful

module FreeMonad =
    type ToyF<'T> =
    | Output of 'T * ToyF<'T>
    | Bell of ToyF<'T>
    | Done

    let fmap f toy = 
        match toy with
        | Output (x, next) -> Output (x, (f next))
        | Bell next -> Bell (f next)
        | Done -> Done

    type Free<'F,'R> = 
        | Free of 'F * (Free<'F,'R>) 
        | Pure of 'R

    type Toy<'T> = Free<ToyF<'T>,'T>

    let empty = Pure ()
    let output x = Free (Output (x, Pure ()))
    let bell = Free (Bell (Pure ()))

    let liftF cmd = Free (fmap Pure, command) 

    let output x = Free (Output x ())

module StreamWorker =
    type StreamWorkerState<'T> =
    | Result of 'T
    | ReadStreamItem of int * string * StreamWorkerState<'T>
    | WriteStreamItem of int * string * StreamWorkerState<'T>

    let bind

    type StreamWorkerBuilder() =
        member x.Return(()) = StreamWorkerState.None 
        member x.Bind (inp:StreamWorkerState<'T>, body : 'T -> StreamWorkerState<'U>) : StreamWorkerState<'U> = 
          bind(inp, body)

    let streamWorker = StreamWorkerBuilder()