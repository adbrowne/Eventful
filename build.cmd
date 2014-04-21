powershell -NoProfile -ExecutionPolicy Bypass -Command "& '%~dp0\build\build.ps1' %*;"
exit /B %errorlevel%