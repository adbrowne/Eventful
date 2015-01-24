namespace Eventful

[<CLIMutable>]
type EventPosition = {
    Commit: int64
    Prepare : int64
}
with static member Start = { Commit = 0L; Prepare = 0L }
     // none indicates a parsing failure
     static member ParseToken (value : string) : EventPosition option =
        let components = value.Split([|"::"|], System.StringSplitOptions.None)
        match components with
        | [|Integer64 commit; Integer64 prepare|] ->
            Some { Commit =  commit; Prepare = prepare }
        | _ -> None
            
     member x.BuildToken () = sprintf "%020d::%020d" x.Commit x.Prepare

type IBulkMessage = 
    abstract member GlobalPosition : EventPosition option
    abstract member EventType : System.Type