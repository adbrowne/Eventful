namespace BookLibrary

open System
open EventStore.ClientAPI
open Eventful
open Eventful.EventStore
open FSharpx

type BookLibrarySystem (system : BookLibraryEventStoreSystem) = 
    interface IBookLibrarySystem with
        member x.RunCommand cmd =
            system.RunCommand () cmd

        member x.RunCommandTask cmd =
            system.RunCommand () cmd
            |> Async.StartAsTask

type RavenConfig = {
    Server : string
    Port : int
    Database : string
}

type EventStoreConfig = {
    Server : string
    TcpPort : int
    Username : string
    Password: string
}

type WebServerConfig = {
    Server : string
    Port : int
}

type ApplicationConfig = {
    Raven: RavenConfig
    EventStore : EventStoreConfig
    WebServer : WebServerConfig
}