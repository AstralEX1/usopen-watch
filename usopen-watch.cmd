@echo off
node --env-file-if-exists="%~dp0.env" "%~dp0usopen-watch.mjs" %*
