namespace Eventful

type IWakeupMonitor =
    abstract member Start : unit -> unit
    abstract member Stop : unit -> unit