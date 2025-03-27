# Project Setup and Usage

## Application summary
This is a microservices based application, using RabbitMQ as the message broker and RPC (Remote Procedure Call) as request protocol.

- Service A with be the responsible of receiving the CLI application messages and process them.

- Service B will be the responsible of processing errors coming from Service A and showing them to the user.

## How to Start the Application
To start the application, navigate to the project directory and run the following command:
```sh
docker-compose up --build
```

## How to Send Requests Using the CLI
1. Once the Docker cluster is up and running, check that RabbitMQ is ready by running:
   ```sh
   docker logs service_a -f
   ```
2. Execute the sender application using the following command:
   ```sh
   docker exec -it sender python sender.py
   ```
3. If you want to check errors, run the following command:
   ```sh
   docker logs service_b -f
   ```

## Dependencies
- Docker must be installed on your system.

