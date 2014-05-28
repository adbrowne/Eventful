$version = "0.0.1.6"
properties {
  $slnPath = "..\src\Eventful.sln"
}

task default -depends MsBuildRelease

task Clean {
  if (Test-Path .\package\lib) {
  	remove-item .\package\lib -force -recurse
  }
  remove-item Eventful*.nupkg
  exec { msbuild /t:Clean $slnPath }
}

task MsBuildDebug {
  exec { msbuild /t:Build $slnPath -p:Configuration=Debug /maxcpucount:8 /verbosity:quiet}
}

task MsBuildRelease {
  exec { msbuild /t:Build $slnPath -p:Configuration=Release /maxcpucount:8 /verbosity:quiet }
}

task Test -depends MsBuildRelease {
	exec { & { ..\src\packages\xunit.runners.1.9.2\tools\xunit.console.clr4.exe .\Release\Eventful.Tests.dll }}
}

task Package -depends Clean, MsBuildRelease {
  New-Item -force .\package\lib\net45 -itemtype directory
  Copy-Item .\Release\Eventful.dll .\package\lib\net45
  Copy-Item .\Release\Eventful.EventStore.dll .\package\lib\net45
  exec { & {.\tools\nuget\nuget.exe pack .\package\Eventful.nuspec -version $version }}
}

task PackagePush -depends Package {
  exec { & {.\tools\nuget\nuget.exe push Eventful.$version.nupkg }}
}

task ? -Description "Helper to display task info" {
	Write-Documentation
}