@echo off
:: This is file is to be run by simply "tune3.bat" in a Cmd Prompt.
:: It allows 8G RAM when running the tune3 script for better performance on large datasets.
node --max-old-space-size=8192 bt/tune3.js %*
:: OR: node --max-old-space-size=8192 bt/tune3.js