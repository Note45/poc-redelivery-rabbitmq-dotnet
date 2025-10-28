# Redelivery RabbitMQ Project
This project is a case study where I implemented retry and redelivery strategies using older versions of MassTransit with RabbitMQ, in which automated plugins for these features are not available.

## 🚀 Goals

- Validate the use of retry and redelivery strategies in older versions of MassTransit with RabbitMQ.
- Simulate write and read operations based on events.
- Provide a starting point for applications adopting retry and redelivery strategies. 

## 🔧 Tech Stack

- [.NET](https://dotnet.microsoft.com/)
- **MassTransit**
- **RabbitMQ**  

## ⚙️ How to Run

### Prerequisites
- [.NET 9+ SDK](https://dotnet.microsoft.com/download)
- [RabbitMQ](https://www.rabbitmq.com/docs/install-windows#installer) running locally (can be via Docker).

### Steps
1. Clone the repository:
```bash
   git clone https://github.com/Note45/poc-redelivery-rabbitmq-dotnet.git
   cd poc-redelivery-rabbitmq-dotnet
```

2. Run RabbitMQ service

3. Run the project:

```bash 
  dotnet run --project RedeliveryProject/RedeliveryProject.csproj 
```

4. The Redelivery RabbitMQ Project logs in terminal will show the stages
