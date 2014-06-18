#!/bin/sh

mono --runtime=v4.0 src/.nuget/NuGet.exe restore src/Eventful.sln
xbuild src/Eventful.sln
