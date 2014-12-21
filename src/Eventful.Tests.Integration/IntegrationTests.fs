namespace Eventful.Tests.Integration

open Eventful
open System.IO
open System.Net
open System.Net.Sockets
open System.Diagnostics

module IntegrationTests =
    let log = createLogger "Eventful.IntegrationTests"

    let buildDirectoryPath = DirectoryInfo(Directory.GetCurrentDirectory()).Parent.FullName

    let bookLibraryExecutableDirectory = Directory.GetCurrentDirectory()

    let findFreeTcpPort () =
        let l = new TcpListener(IPAddress.Loopback, 0)
        l.Start()
        let port = (l.LocalEndpoint :?> IPEndPoint).Port
        l.Stop()
        port

    let startProcess executable arguments =
        let startInfo = 
            System.Diagnostics.ProcessStartInfo(
                executable, 
                arguments,
                CreateNoWindow = true,
                UseShellExecute = false,
                RedirectStandardOutput = true)

        Process.Start(startInfo)

    let runUntilSuccess maxTries f =
        let rec loop attempt =
            try 
                f()
            with | _ ->
                if (attempt < maxTries) then
                    loop (attempt + 1) 
                else
                    reraise()
        loop 0