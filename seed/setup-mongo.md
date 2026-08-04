# Setup: MongoDB (change-stream ready)

Pulse's Mongo source uses **change streams**, which require the server to be a
member of a **replica set** — even a single-node one. A standalone `mongod` will
fail on `startChangeStream`.

## One-liner (Docker, single-node replica set)

```bash
docker run -d --name pulse-mongo \
  -p 27017:27017 \
  mongo:7 --replSet rs0

# wait for mongod to accept connections, then initialise the set:
sleep 5
docker exec pulse-mongo mongosh --quiet \
  --eval 'rs.initiate({_id:"rs0", members:[{_id:0,host:"127.0.0.1:27017"}]})'
```

Verify:

```bash
docker exec pulse-mongo mongosh --quiet --eval 'rs.status().set'   # -> rs0
```

## Or via docker compose

```bash
docker compose -f seed/compose.yaml up -d mongo
docker compose -f seed/compose.yaml exec mongo mongosh --eval 'rs.initiate()'
```

## Notes

- The test server connects to `mongodb://localhost:27017`, database `pulse`
  (override with `PULSE_MONGO_URI` / `PULSE_MONGO_DB`).
- The `orders` collection and indexes are created automatically by the seed tool /
  test server on first run; nothing else is needed.
- `Pulse.TestApp.Seed verify-setup` reports whether the replica set is detected.