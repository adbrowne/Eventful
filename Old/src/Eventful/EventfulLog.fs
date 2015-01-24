namespace Eventful

open Serilog

type EventfulLog () =
    static let mutable log : Serilog.ILogger = Log.Logger

    static member SetLog newLog = log <- newLog

    static member ForContext (context : string) =
        log.ForContext("SourceContext", context)