# Trading Daily Report

## Setup

1. Clone the repo

```bash
git clone https://github.com/gunjanmistry08/trading-daily-report.git
```

2. Install the dependencies

```bash
go mod tidy
```

3. Database Setup

```bash
docker run -d \
  --name mongodb \
  -p 27017:27017 \
  -e MONGO_INITDB_ROOT_USERNAME=root \
  -e MONGO_INITDB_ROOT_PASSWORD=password \
  -v mongo-data:/data/db \
  mongo:7
```

4. `seed.go` is used to seed the database... script needs correction

```bash
go run seed.go
```

5. Run the Server

```bash
go run main.go
```

can be run in development using AIR

```bash
air
```

installing AIR

```bash
go install github.com/cosmtrek/air@v1.52.0
```
