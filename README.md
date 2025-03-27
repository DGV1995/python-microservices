# Project Setup and Usage

## Application summary
This is a microservices based application, using RabbitMQ as the message broker and RPC (Remote Procedure Call) as request protocol.

- Service A with be the responsible of receiving the CLI application messages and process them.

- Service B will be the responsible of processing errors coming from Service A and showing them to the user.

## How to Start the Application
To start the application, navigate to the project directory and run the following command:
```sh
start_app.bat
```

Once the Docker cluster is up and running, a new CMD window with the main application script will be open. You will be asked for some parameters to run the process.

## Logs
1. You can check ServiceA logs running the following command
   ```sh
   docker logs service_a -f
   ```
2. You can check ServiceB logs running the following command
   ```sh
   docker logs service_b -f
   ```

## Dependencies
- Docker must be installed on your system.

