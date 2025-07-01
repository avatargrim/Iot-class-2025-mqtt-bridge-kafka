# 📡 IoT MQTT Bridge to Kafka

A Dockerized sample IoT subscriber application that simulates sensor data and publishes it to a broker or stream processor (e.g., MQTT, Kafka).

```
iot-class-2025-mqtt-bridge-kafka/
├── app/
│   ├── Dockerfile
│   ├── main.py
│   ├── requirements.txt
│   └── Readme.md
├── .env
├── .gitignore
└── docker-compose.yml
```

---

## 🛠️ Prerequisites

Ensure the following tools are installed on your system:

* [Docker](https://docs.docker.com/get-docker/)
* [Git](https://git-scm.com/downloads)

---

## 🚀 Get the Application

Clone the repository using Git:

```bash
git clone https://github.com/hanattaw/Iot-class-2025-mqtt-bridge-kafka
cd Iot-class-2025-mqtt-bridge-kafka
```

---

## 💾 **Starting Container**
```bash
# change to directory mqtt-bridge-kafka
$ cd ~/Iot-class-2025-mqtt-bridge-kafka

# build and start container
$ docker compose up --build 

```
|Option	|Default	|Description|
|--|--|--|
|--build		| |Build images before starting containers|

---


## 💾 **Stop and remove containers, networks**
```bash
# change to directory mqtt-bridge-kafka
$ cd ~/Iot-class-2025-mqtt-bridge-kafka

# build and start container
$ docker compose down --volumes --remove-orphans --rmi local

```

|Option	|Default	|Description|
|--|--|--|
|--remove-orphans		| |Remove containers for services not |defined in the Compose file|
|--rmi		| |Remove images used by services. "local" remove |only images that don't have a custom tag ("local"||"all")|
|-t, --timeout		| |Specify a shutdown timeout in seconds|
|-v, --volumes		| |Remove named volumes declared in the |"volumes" section of the Compose file and anonymous |volumes attached to containers|

---