@echo off
:: Build and deploy app in the background
docker-compose up --build -d

:: Wait some time until running the main process
timeout /t 30

:: Open a new CMD and execute the main process
start cmd /k "docker exec -it sender python sender.py"

pause