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

task RestorePackages {
  exec { & {.\tools\nuget\nuget.exe restore ..\src\Eventful.sln }}
}

task Test -depends MsBuildRelease {
	exec { & { ..\src\packages\xunit.runners.1.9.2\tools\xunit.console.clr4.exe .\Release\Eventful.Tests.dll }}
}

task Package -depends Clean, RestorePackages, MsBuildRelease, CreateNugetPackage {
}

task CreateNugetPackage {
  # $version = Get-Item .\Release\Eventful.dll | % {$_.versioninfo.ProductVersion}
  $version = "0.0.2-beta"
  Write-Host "Creating directory"
  New-Item -force .\package\lib\net45 -itemtype directory
  Write-Host "Copying .\Release\Eventful.dll"
  Copy-Item .\Release\Eventful.dll .\package\lib\net45
  Write-Host "Copying .\Release\Eventful.EventStore.dll"
  Copy-Item .\Release\Eventful.EventStore.dll .\package\lib\net45
  Write-Host "Copying .\Release\Eventful.RavenDB.dll"
  Copy-Item .\Release\Eventful.RavenDB.dll .\package\lib\net45
  Write-Host "Running nuget"
  exec { & {.\tools\nuget\nuget.exe pack .\package\Eventful.nuspec -version $version -Verbosity detailed}}
  Write-Host "Copying output"
  Copy-Item Eventful.$version.nupkg output.nupkg
}

task PackagePush -depends Package {
  # $version = Get-Item .\Release\Eventful.dll | % {$_.versioninfo.ProductVersion}
  $version = "0.0.2-beta"
  exec { & {.\tools\nuget\nuget.exe push Eventful.$version.nupkg }}
}

task ? -Description "Helper to display task info" {
	Write-Documentation
}