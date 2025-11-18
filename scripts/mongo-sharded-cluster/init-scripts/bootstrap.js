// LIGHT bootstrap.js

function waitFor(host, port) {
  print(`Waiting for ${host}:${port} ...`);
  let ready = false;
  while (!ready) {
    try {
      const c = new Mongo(`${host}:${port}`);
      c.getDB("admin").runCommand({ ping: 1 });
      ready = true;
    } catch (e) {
      sleep(1000);
    }
  }
  print(`${host}:${port} is UP`);
}

// ------------------------------
// Wait for config + shard
// ------------------------------
waitFor("config", 27019);
waitFor("shard1", 27018);

sleep(2000);

// ------------------------------
// Connect to mongos
// ------------------------------
const mongos = new Mongo("mongos:27017");
const admin = mongos.getDB("admin");

// ------------------------------
// Add shard (IMPORTANT: must include RS name)
// ------------------------------
print("Adding shard...");
admin.runCommand({
  addShard: "shard1/shard1:27018"
});

// ------------------------------
// Enable sharding for DB
// ------------------------------
print("Enabling sharding for trading DB...");
admin.runCommand({ enableSharding: "trading" });

// ------------------------------
// Shard collections
// ------------------------------
print("Sharding collections...");

admin.runCommand({
  shardCollection: "trading.orders",
  key: { _id: "hashed" }
});

admin.runCommand({
  shardCollection: "trading.trades",
  key: { order_id: "hashed" }
});

admin.runCommand({
  shardCollection: "trading.positions",
  key: { client_id: "hashed" }
});

// strategy_info is NOT sharded

print("\n✔✔✔ LIGHT MongoDB Sharded Cluster Ready ✔✔✔");
