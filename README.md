Fraud Detection System
# Fraud Detection System with Kafka and AI

A real-time fraud detection system built using **Java, Spring Boot, Apache Kafka, and TensorFlow**. The system receives transaction data, sends it through a Kafka-based streaming pipeline, and uses a trained machine learning model to classify transactions as **SAFE** or **FRAUD**.

## 🚀 Project Overview

This project demonstrates how **Artificial Intelligence and event-driven architecture** can be combined to detect potentially fraudulent financial transactions in real time.

The application is divided into two Spring Boot services:

* **Fraud Detection Producer** – receives transaction requests and publishes transaction data to Kafka.
* **Fraud Detection Consumer** – consumes transactions from Kafka and sends the transaction data to a TensorFlow model for fraud prediction.

Apache Kafka acts as the communication layer between the producer and consumer, allowing the system to process transactions asynchronously.

## 🏗️ System Architecture

```text
                    Transaction Request
                           │
                           ▼
                ┌─────────────────────┐
                │  Spring Boot        │
                │  Producer           │
                │  Port: 8087         │
                └──────────┬──────────┘
                           │
                           ▼
                ┌─────────────────────┐
                │    Apache Kafka     │
                │                     │
                │ fraud-detection-    │
                │ topic               │
                └──────────┬──────────┘
                           │
                           ▼
                ┌─────────────────────┐
                │  Spring Boot        │
                │  Consumer           │
                │  Port: 8088         │
                └──────────┬──────────┘
                           │
                           ▼
                ┌─────────────────────┐
                │   TensorFlow Model  │
                │                     │
                │ Fraud Prediction    │
                └──────────┬──────────┘
                           │
                           ▼
                     SAFE / FRAUD
```

## ✨ Features

* Real-time transaction processing
* Kafka-based event streaming
* Spring Boot REST APIs
* Machine learning-based fraud detection
* TensorFlow SavedModel integration
* Producer-consumer architecture
* Configurable fraud probability threshold
* Transaction amount and transaction time as model inputs
* Asynchronous communication using Apache Kafka

## 🛠️ Technologies Used

| Technology    | Purpose                                    |
| ------------- | ------------------------------------------ |
| Java          | Backend programming                        |
| Spring Boot   | REST APIs and application framework        |
| Apache Kafka  | Real-time transaction streaming            |
| TensorFlow    | Machine learning fraud detection           |
| Maven         | Dependency management and build automation |
| Apache Tomcat | Embedded web server                        |
| Git/GitHub    | Version control                            |

## 📁 Project Structure

```text
FraudDetectionBased-main/
│
├── fraud-detection-producer/
│   ├── src/
│   │   ├── main/
│   │   │   ├── java/
│   │   │   │   └── com.example.frauddetectionproducer/
│   │   │   │       ├── FraudDetectionProducerApplication.java
│   │   │   │       ├── FraudDetectionProducerController.java
│   │   │   │       └── KafkaProducerService.java
│   │   │   │
│   │   │   └── resources/
│   │   │       └── application.properties
│   │
│   └── pom.xml
│
├── fraud-detection-consumer/
│   ├── src/
│   │   ├── main/
│   │   │   ├── java/
│   │   │   │   └── com.example.frauddetectionconsumer/
│   │   │   │       ├── FraudDetectionConsumerApplication.java
│   │   │   │       ├── FraudDetectionConsumerController.java
│   │   │   │       ├── FraudDetectionService.java
│   │   │   │       └── KafkaConsumerService.java
│   │   │   │
│   │   │   └── resources/
│   │   │       └── application.properties
│   │
│   └── pom.xml
│
├── serving/
│   └── models/
│       └── fraud_model/
│           └── 1/
│               ├── saved_model.pb
│               └── variables/
│
├── fraud_detection_model.ipynb
├── images/
└── README.md
```

## 🔄 How the System Works

### 1. Send a Transaction

A transaction is submitted to the Producer service with:

* Transaction amount
* Transaction time

Example:

```text
Amount = 3000
Time = 23
```

### 2. Producer Sends Data to Kafka

The Spring Boot Producer receives the transaction and publishes it to the Kafka topic:

```text
fraud-detection-topic
```

Kafka acts as the message broker between the two applications.

### 3. Consumer Receives the Transaction

The Consumer service listens to the Kafka topic.

When a new transaction arrives, the consumer retrieves the transaction amount and time.

### 4. TensorFlow Performs Prediction

The transaction data is passed to the trained TensorFlow model.

The model generates a fraud prediction score.

The application then compares the score against the configured fraud threshold.

```text
Score < 0.5  → SAFE
Score >= 0.5 → FRAUD
```

The threshold can be configured in the application properties.

### 5. Display the Result

The Consumer logs the prediction:

```text
Transaction [Amount: 3000.00, Time: 23.00]
Score: 0.50
FRAUD
```

## 🧠 Machine Learning Model

The machine learning model is developed using Python and TensorFlow.

The training workflow is available in:

```text
fraud_detection_model.ipynb
```

The trained model is exported as a TensorFlow SavedModel and stored under:

```text
serving/models/fraud_model/1/
```

The Java Consumer loads this model during application startup.

## ⚙️ Configuration

### Producer

The Producer runs on:

```text
http://localhost:8087
```

Its Kafka configuration is:

```properties
spring.kafka.bootstrap-servers=localhost:9092
server.port=8087
```

### Consumer

The Consumer runs on:

```text
http://localhost:8088
```

Its Kafka configuration is:

```properties
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=fraud-consumer-group
server.port=8088
```

## 📋 Prerequisites

Before running the project, make sure you have:

* Java JDK installed
* Apache Maven or Maven Wrapper
* Apache Kafka
* A running Kafka broker on port `9092`
* IntelliJ IDEA or another Java IDE
* Python and TensorFlow if you want to retrain the model

## ▶️ Running the Project

### Step 1: Start Kafka

Start your Kafka server and make sure it is available at:

```text
localhost:9092
```

### Step 2: Start the Producer

Open a terminal inside:

```text
fraud-detection-producer
```

Run:

```cmd
mvnw.cmd spring-boot:run
```

The Producer should start on:

```text
http://localhost:8087
```

### Step 3: Start the Consumer

Open another terminal inside:

```text
fraud-detection-consumer
```

Run:

```cmd
mvnw.cmd spring-boot:run
```

The Consumer should start on:

```text
http://localhost:8088
```

### Step 4: Send a Transaction

You can test the system using:

```cmd
curl "http://localhost:8087/send-transaction?amount=3000&time=23"
```

The Producer sends the transaction to Kafka.

The Consumer receives it and performs the AI prediction.

## 🧪 Example

### Input

```text
Amount: 3000
Time: 23
```

### Producer Response

```text
Transaction was processed successfully
```

### Consumer Output

```text
Transaction [Amount: 3000.00, Time: 23.00]
Score: 0.50
FRAUD
```

The exact prediction depends on the trained machine learning model.

## 🔐 Fraud Threshold

The Consumer contains a configurable fraud threshold:

```properties
fraud.threshold=0.5
```

The model's prediction score is compared against this threshold.

```text
Prediction Score >= 0.5 → FRAUD

Prediction Score < 0.5 → SAFE
```

The threshold can be adjusted depending on the desired sensitivity of the fraud detection system.

## 📊 Kafka Workflow

The Kafka pipeline follows:

```text
Producer
   │
   │ Transaction
   ▼
Kafka Topic
   │
   │ Transaction Event
   ▼
Consumer
   │
   ▼
TensorFlow Model
   │
   ▼
Prediction
```

This architecture allows the system to process transactions independently and provides a foundation for scaling the fraud detection service.

## 🔮 Future Improvements

The project can be extended with several features:

* Build a modern web dashboard
* Add a React frontend
* Display real-time transaction history
* Add fraud statistics and charts
* Store transactions in a database
* Add authentication and authorization
* Add transaction IDs
* Add customer/user information
* Add Kafka producer and consumer monitoring
* Improve the machine learning model
* Add more transaction features
* Deploy the system using Docker
* Deploy Kafka and services to the cloud
* Add real-time alerts for fraudulent transactions

## 🎯 Learning Objectives

This project demonstrates practical implementation of:

* Spring Boot microservices
* REST API development
* Apache Kafka
* Event-driven architecture
* Producer-consumer communication
* Machine learning integration with Java
* TensorFlow SavedModel
* Maven
* Real-time data processing

## 👨‍💻 Author

**Aryan Saroj**

B.Tech Computer Science Engineering

## 📄 License

This project is intended for educational and learning purposes.
