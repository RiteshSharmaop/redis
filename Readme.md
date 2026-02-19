# 🚀 My Cpp Redis (Client + Server)

---

A **complete Redis ecosystem built from scratch in C++** — featuring both a **Redis-compatible server** and a **fully functional Redis CLI client**. This project demonstrates low-level networking, protocol design, concurrency, data structures, and persistence, closely mirroring how real Redis works internally.

> Think of this as **Redis re‑implemented in C++**, end‑to‑end.

---

## 📌 Overview

This repository combines two tightly-coupled projects:

### 🔹 1. Redis Server (`my_redis_server`)

A lightweight, Redis-compatible **in‑memory data store** written in **C++**, supporting:

* Multiple clients over TCP
* RESP protocol parsing
* Strings, Lists, and Hashes
* Expiration (TTL)
* Periodic disk persistence

### 🔹 2. Redis CLI Client (`my_redis_cli`)

A **Redis Command Line Interface** written in **C++**, similar to the official `redis-cli`, supporting:

* Interactive REPL mode
* One‑shot command execution
* RESP response parsing
* IPv4 / IPv6 TCP networking

Together, they form a **full client‑server Redis implementation**.

---

## 🧠 What You’ll Learn From This Project

* Low-level **TCP socket programming** (Berkeley sockets)
* **RESP (Redis Serialization Protocol)** — encoding & decoding
* **Multithreading & synchronization** in C++
* Designing an **in-memory database**
* Implementing **Redis commands**
* **Persistence** and graceful shutdowns
* Clean **OOP and modular architecture**

---

## 📂 Repository Structure

```
├── client/                 # Redis CLI Client
│   ├── include/
│   │   ├── RedisClient.h
│   │   ├── CommandHandler.h
│   │   └── ResponseParser.h
│   ├── src/
│   │   ├── RedisClient.cpp
│   │   ├── CommandHandler.cpp
│   │   ├── ResponseParser.cpp
│   │   └── main.cpp
│   ├── Makefile
│   └── README.md
│
├── server/                 # Redis Server
│   ├── include/
│   │   ├── RedisServer.h
│   │   ├── RedisDatabase.h
│   │   └── RedisCommandHandler.h
│   ├── src/
│   │   ├── RedisServer.cpp
│   │   ├── RedisDatabase.cpp
│   │   ├── RedisCommandHandler.cpp
│   │   └── main.cpp
│   ├── Concepts,UseCases&Tests.md
│   ├── test_all.sh
│   ├── Makefile
│   └── README.md
│
└── README.md               # (You are here)
```

---

## ⚙️ Requirements

* **C++17 or later**
* **G++ Compiler**
* **Make**
* Linux or macOS (Windows via WSL recommended)

---

## 🔨 Build Instructions

### Build Redis Server

```bash
cd server
make
```

Executable generated:

```
my_redis_server
```

### Build Redis CLI Client

```bash
cd client
make
```

Executable generated:

```
bin/my_redis_cli
```

---

## ▶️ Usage

### 1️⃣ Running the Redis Server

```bash
./my_redis_server        # Default port 6379
./my_redis_server 6380   # Custom port
```

On startup:

* Loads data from `dump.my_rdb` if present
* Starts background persistence every **5 minutes**

Graceful shutdown:

```
Ctrl + C   # Saves DB immediately
```

---

### 2️⃣ Using the Redis CLI Client

#### Interactive REPL Mode

```bash
./bin/my_redis_cli
```

Example:

```
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET mykey hello
OK
127.0.0.1:6379> GET mykey
"hello"
127.0.0.1:6379> quit
Goodbye.
```

#### One‑Shot Command Mode

```bash
./bin/my_redis_cli SET mykey "hello"
./bin/my_redis_cli GET mykey
```

#### Connect to Custom Host / Port

```bash
./bin/my_redis_cli -h 192.168.1.10 -p 6380
```

---

## 🧾 Supported Redis Commands

### 🔹 Common

* `PING` → PONG
* `ECHO <msg>`
* `FLUSHALL`

### 🔹 Key / Value

* `SET <key> <value>`
* `GET <key>`
* `KEYS *`
* `TYPE <key>`
* `DEL <key>` / `UNLINK <key>`
* `EXPIRE <key> <seconds>`
* `RENAME <old> <new>`

### 🔹 Lists

* `LPUSH / RPUSH <key> <v1> [v2 ...]`
* `LPOP / RPOP <key>`
* `LGET <key>`
* `LLEN <key>`
* `LINDEX <key> <index>`
* `LSET <key> <index> <value>`
* `LREM <key> <count> <value>`

### 🔹 Hashes

* `HSET <key> <field> <value>`
* `HGET <key> <field>`
* `HEXISTS <key> <field>`
* `HDEL <key> <field>`
* `HLEN <key>`
* `HKEYS <key>`
* `HVALS <key>`
* `HGETALL <key>`
* `HMSET <key> <f1> <v1> [f2 v2 ...]`

---

## 🏗️ Architecture & Design

### Redis Server

* **Thread-per-client model** using `std::thread`
* **Thread-safe database** guarded by `std::mutex`
* **Singleton database instance**
* **Lazy expiration** with TTL map
* **Custom RESP parser** (inline + array formats)
* **Text-based RDB persistence** (`dump.my_rdb`)

### Redis CLI Client

* **TCP networking** with IPv4 & IPv6 support
* **RESP command encoder**
* **RESP response parser**
* **REPL shell** similar to official `redis-cli`
* **Clean OOP separation of concerns**

---

## 🧪 Testing

* Manual testing via CLI
* Automated test script:

```bash
./test_all.sh
```

* Detailed test cases and concepts are documented in:

```
Concepts,UseCases&Tests.md
```

---

## 🎥 Learning Resource

📺 **Step‑by‑Step Article**
[Build Your Own Redis in C++](https://build-your-own.org/redis)
[Build Your Own Redis in C++ Codecrafter](https://app.codecrafters.io/courses/redis/introduction)


---

## 👨‍💻 Authors & Credits

* **Ritesh Sharma**

---

## ❤️ Final Note

This project is built with **curiosity**, **coffee**, and a deep love for **systems programming**.

If you want to understand *how Redis actually works under the hood*, this repository is for you.

**Happy hacking! ☕🚀**
