// source: https://gist.github.com/haf/4252121#file-topshelf-fsharp-fs
namespace Topshelf

[<AutoOpen>]
module Topshelf =

  open System
  open Topshelf.HostConfigurators
  open Topshelf.Runtime
  open Topshelf.Common.Logging

  let configureTopShelf f =
    HostFactory.Run(new Action<_>(f)) |> int
  
  let addCommandLineDefinition (conf : HostConfigurator) str action =
    conf.AddCommandLineDefinition(str, new Action<_>(action))

  let addCommandLineSwitch (conf : HostConfigurator) str action =
    conf.AddCommandLineSwitch(str, new Action<_>(action))
   
  let addDependency (conf : HostConfigurator) depName =
    conf.AddDependency depName |> ignore

  let beforeInstall (conf : HostConfigurator) f =
    conf.BeforeInstall(new Action<InstallHostSettings>(f)) |> ignore

  let afterInstall (conf : HostConfigurator) f =
    conf.AfterInstall(new Action<InstallHostSettings>(f)) |> ignore
  
  let applyCommandLine (conf : HostConfigurator) str =
    conf.ApplyCommandLine str
    
  let beforeUninstall (conf : HostConfigurator) f =
    conf.BeforeUninstall(new Action(f)) |> ignore

  let afterUninstall (conf : HostConfigurator) f =
    conf.AfterUninstall(new Action(f)) |> ignore

  let dependsOn (conf : HostConfigurator) name =
    conf.DependsOn name |> ignore

  let dependsOnEventLog (conf : HostConfigurator) =
    conf.DependsOnEventLog() |> ignore

  let dependsOnIIS (conf : HostConfigurator) =
    conf.DependsOnIis() |> ignore

  let dependsOnMsSQL (conf : HostConfigurator) = 
    conf.DependsOnMsSql() |> ignore

  let dependsOnRabbitMQ (conf : HostConfigurator) =
    "RabbitMQ" |> dependsOn conf

  let dependsOnMSMQ (conf : HostConfigurator) =
    conf.DependsOnMsmq() |> ignore

  let disabled (conf : HostConfigurator) =
    conf.Disabled() |> ignore

  let enablePauseAndContinue (conf : HostConfigurator) =
    conf.EnablePauseAndContinue()

  let enableServiceRecovery (conf : HostConfigurator) f =
    conf.EnableServiceRecovery(new Action<_>(f)) |> ignore

  let enableShutdown (conf : HostConfigurator) =
    conf.EnableShutdown()

  let loadHelpTextPrefix (conf : HostConfigurator) asm str =
    conf.LoadHelpTextPrefix( asm, str ) |> ignore

  let runAs (conf : HostConfigurator) usr pwd =
    conf.RunAs(usr, pwd) |> ignore
    
  let runAsNetworkService (conf : HostConfigurator) =
    conf.RunAsNetworkService() |> ignore

  let runAsLocalSystem (conf : HostConfigurator) = 
    conf.RunAsLocalSystem() |> ignore

  let runAsLocalService (conf : HostConfigurator) =
    conf.RunAsLocalService() |> ignore
  
  let runAsPrompt (conf : HostConfigurator) =
    conf.RunAsPrompt() |> ignore
    
  let description (conf : HostConfigurator) str = 
    conf.SetDescription str

  let displayName (conf : HostConfigurator) str =
    conf.SetDisplayName str

  let serviceName (conf : HostConfigurator) str =
    conf.SetServiceName str

  let instanceName (conf : HostConfigurator) str =
    conf.SetInstanceName str

  let helpTextPrefix (conf : HostConfigurator) str = 
    conf.SetHelpTextPrefix str |> ignore
    
  let startAutomatically (conf : HostConfigurator) =
    conf.StartAutomatically() |> ignore

  let startAutomaticallyDelayed (conf : HostConfigurator) =
    conf.StartAutomaticallyDelayed() |> ignore

  let startManually (conf : HostConfigurator) =
    conf.StartManually() |> ignore

  let useEnvBuilder (conf : HostConfigurator) f =
    conf.UseEnvironmentBuilder(new EnvironmentBuilderFactory(f))
  
  let useHostBuilder (conf : HostConfigurator) f =
    conf.UseHostBuilder(new HostBuilderFactory(f))

  let useServiceBuilder (conf : HostConfigurator) f =
    conf.UseServiceBuilder(new ServiceBuilderFactory(f))
  
  let useTestHost (conf : HostConfigurator) =
    conf.UseTestHost() |> ignore

  let service (conf : HostConfigurator) (fac : (unit -> 'a)) =
    let service' = conf.Service : Func<_> -> HostConfigurator
    service' (new Func<_>(fac)) |> ignore

  let serviceControl (start : HostControl -> bool) (stop : HostControl -> bool) =
    { new ServiceControl with
        member x.Start hc =
          start hc
        member x.Stop hc =
          stop hc }