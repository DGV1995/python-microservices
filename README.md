# Project Setup and Usage

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

## Dependencies
- Docker must be installed on your system.

