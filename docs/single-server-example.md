# A single server, end to end

The smallest real deployment: one `nisaba-server` process owning one
directory, the HTTP front end in front of it, and `curl`. Every response
shown below is pasted from a live run of exactly these commands.

This is a worked example, not the reference — the server's flags and
directory layout are [`db-server.md`](db-server.md)'s, the URL grammar
and every rule behind it [`http-front.md`](http-front.md)'s.

## 1. The server — one process, one root directory

```sh
./build/build-server.sh --native      # → build/lib/nisaba-server

mkdir -p ~/nisaba-data && cd ~/nisaba-data
/path/to/repo/build/lib/nisaba-server --port 8097
# nisaba: serving 127.0.0.1:8097 (max 64 clients, idle timeout 60s)
```

The **working directory is the root** — there is deliberately no
`--dir` flag ([`db-server.md`](db-server.md) says why). Each database
becomes a subdirectory; an empty directory is an empty instance.

The deployment target is wasip2 under wasmtime — same code, same root
rule, with the directory granted by the host instead of `cd`:

```sh
./build/build-server.sh              # → build/lib/nisaba-server-wasip2.wasm
wasmtime run -S inherit-network --dir ~/nisaba-data::. \
  build/lib/nisaba-server-wasip2.wasm --port 8097
```

## 2. The HTTP front end — HTTP on one side, binjson frames on the other

```sh
node bin/http-front.js --listen 8080 --server 127.0.0.1:8097
# db-http: serving http on 127.0.0.1:8080 for 127.0.0.1:8097
```

No engine in this process: it maps HTTP onto the server's own wire and
everything it answers is the server's answer. Both listeners default to
loopback; widen consciously.

## 3. curl

The path owns `op`, `db` and `coll` — `POST /<op>`, `POST /db/<db>/<op>`,
`POST /db/<db>/<coll>/<op>` — and the JSON body is the rest of the wire
request verbatim. A body that restates a path fact is refused (400).

```sh
curl -s -X POST localhost:8080/ping
# {"ok":true,"pong":true}
```

(`-X POST` matters: a bare `curl` sends GET, and the only GET route is
`watch`.)

### Writes

An insert whose document has no `_id` gets an ObjectId minted by the
front end — the engine deliberately never invents one — and whatever was
minted is echoed under `minted`, because a caller that was given an
identity has to be told which:

```sh
curl -s localhost:8080/db/shop/orders/insert \
     -d '{"doc": {"sku": "A-17", "qty": 2}}'
# {"ok":true,"result":{"acknowledged":true,"matchedCount":0,"modifiedCount":0,
#   "deletedCount":0,"insertedCount":1,"upsertedId":null},
#  "minted":{"id":{"$oid":"6a71f175370c903dc2657ea2"}}}

curl -s localhost:8080/db/shop/orders/insertMany \
     -d '{"docs": [{"sku": "B-02", "qty": 7}, {"sku": "C-33", "qty": 1}, {"sku": "D-90", "qty": 5}]}'
# {"ok":true,"result":{"acknowledged":true,"insertedCount":3,...},"attempted":3,
#  "upserted":null,"errors":null,
#  "minted":{"ids":{"0":{"$oid":"..."},"1":{"$oid":"..."},"2":{"$oid":"..."}}}}

curl -s localhost:8080/db/shop/orders/update \
     -d '{"filter": {"sku": "A-17"}, "update": {"$set": {"qty": 3, "status": "packed"}}}'
# {"ok":true,"result":{"acknowledged":true,"matchedCount":1,"modifiedCount":1,...}}

curl -s localhost:8080/db/shop/orders/deleteMany \
     -d '{"filter": {"qty": {"$lt": 4}}}'
# {"ok":true,"result":{...,"deletedCount":2,...}}
```

This example writes `shop/orders` into existence: a first insert makes
the collection, and the database with it — no createDatabase, no
mandatory createCollection.

### Reads

```sh
curl -s localhost:8080/db/shop/orders/find \
     -d '{"filter": {"qty": {"$gt": 1}}, "opts": {"sort": {"sku": 1}}}'
# {"ok":true,"docs":[{"_id":{"$oid":"..."},"sku":"A-17","qty":2},
#                    {"_id":{"$oid":"..."},"sku":"B-02","qty":7},
#                    {"_id":{"$oid":"..."},"sku":"D-90","qty":5}]}

curl -s localhost:8080/db/shop/orders/count -d '{"filter": {"qty": {"$gte": 3}}}'
# {"ok":true,"n":3}

curl -s localhost:8080/db/shop/orders/aggregate \
     -d '{"stages": [{"$group": {"_id": "$status", "total": {"$sum": "$qty"}}}]}'
# {"ok":true,"docs":[{"_id":null,"total":13},{"_id":"packed","total":3}]}
```

Values JSON cannot say cross as MongoDB Extended JSON in both
directions: `{"$oid": "<24 hex>"}`, `{"$date": "<ISO 8601>"}`,
`{"$binary": {"base64": "...", "subType": "00"}}`.

### Indexes, and asking the planner

```sh
curl -s localhost:8080/db/shop/orders/createIndex -d '{"keys": {"sku": 1}}'
# {"ok":true,"name":"sku_1"}

curl -s localhost:8080/db/shop/orders/explain -d '{"filter": {"sku": "B-02"}}'
# {"ok":true,"plan":{"source":"equality","index":"sku_1"}}
```

### The instance and the database

```sh
curl -s -X POST localhost:8080/listDatabases
# {"ok":true,"databases":["shop"]}

curl -s -X POST localhost:8080/db/shop/listCollections
# {"ok":true,"collections":["orders"]}
```

### Refusals

A refusal is a response: the wire's own `{ok:false, code, msg}` under an
HTTP status that carries the class (400 wrong request, 503 true-right-now
ask-again, 502 member-gone-mid-request, 404 URL grammar or session).

```sh
curl -s -w ' [http %{http_code}]' localhost:8080/db/shop/orders/frobnicate -d '{}'
# {"ok":false,"code":-41,"msg":"Request names an op this server does not know"} [http 400]
```

### Paged cursors need a session

A cursor is state on one server connection; pooled HTTP requests cannot
hold one, so page inside a session — one real socket, reaped by the
server's own idle timeout if abandoned:

```sh
S=$(curl -s -X POST localhost:8080/session | jq -r .session)

curl -s "localhost:8080/db/shop/orders/find?session=$S" \
     -d '{"filter": {}, "opts": {"batchSize": 2}}'
# {"ok":true,"docs":[...2 docs...],"cursor":1}

curl -s "localhost:8080/db/shop/orders/getMore?session=$S" -d '{"cursor": 1}'
# {"ok":true,"docs":[...2 more...],"cursor":1}

curl -s -X DELETE localhost:8080/session/$S
# {"ok":true,"closed":true}
```

### Change streams are Server-Sent Events

```sh
curl -N localhost:8080/db/shop/orders/watch
```

then, from another shell, `insert` — and the subscribed curl prints:

```
event: watching
data: {"db":"shop","coll":"orders","member":"127.0.0.1:8097"}

event: change
data: {"ns":{"coll":"orders"},"operationType":"insert",
       "documentKey":{"_id":{"$oid":"6a71f1b3423916e0bc8e40ca"}},
       "fullDocument":{"_id":{"$oid":"6a71f1b3423916e0bc8e40ca"},"sku":"E-11","qty":9}}

: keep-alive
```

## What is on disk afterward

```
~/nisaba-data/
└── shop/                        the database this example wrote into existence
    ├── __catalog__.bj
    ├── coll-orders.bj
    ├── coll-orders-journal.bj
    └── idx-orders-sku_1.bj
```

## Footguns met while writing this

- `curl localhost:8080/ping` is a GET and answers "no such route" —
  every op is a POST.
- Quote URLs carrying `?session=...`, and never let JSON stray into a
  URL: curl's brace globbing fans `{...}` out into one request per
  alternative, silently.
