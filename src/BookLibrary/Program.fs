open System
open Eventful
open Serilog

[<EntryPoint>]
let main argv = 

    let log = new LoggerConfiguration()
    let log = log
                .WriteTo.Seq("http://localhost:5341")
                .WriteTo.ColoredConsole()
                .MinimumLevel.Debug()
                .CreateLogger()

    EventfulLog.SetLog log

    let runner = new BookLibrary.BookLibraryServiceRunner()

    runner.Start() |> ignore

    Console.WriteLine("Press 'q' to exit")
    let rec waitForExit () =
        let key = Console.ReadKey()
        match key.KeyChar with
        | 'q' -> ()
        | 'Q' -> ()
        | _ -> waitForExit ()

    waitForExit()

    0