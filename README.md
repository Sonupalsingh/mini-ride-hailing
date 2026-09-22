# Mini Uber/Ola — ride-hailing demo on Kafka + Zookeeper

A minimal ride-hailing app (rider requests, driver accepts, fare, ride
completion) built as an event-driven system on Kafka — the same core
pattern real ride-hailing dispatch systems use, just simplified.

## How the events work

Every ride-related happening is published as an **event** to the Kafka
topic `ride-events`, keyed by `rideId` (so every event about one ride
stays in order on the same partition):

| Event | When | Who gets notified |
|---|---|---|
| `REQUESTED` | Rider requests a ride | Every online driver — "new ride available" |
| `ACCEPTED` | A driver accepts | Every driver — "this ride is gone"; the rider — "your driver is X" |
| `COMPLETED` | Driver marks the ride done | The rider and driver on that ride |
| `CANCELLED` | Rider cancels | Drivers (if it was still open) + the rider |

The server's Kafka **consumer** picks up every event and rebroadcasts it
over Socket.IO to whoever needs to see it — this is the same fan-out
pattern as a pub/sub chat app, just with ride state instead of chat
messages.

## "First driver to accept wins"

When `/api/rides/:id/accept` is called, the server checks the ride's
current status in memory right then — if it's still `requested`, this
driver wins and the ride flips to `accepted`; if not, they get a 409
"ride is no longer available". This is good enough for a **single
server instance**.

**If you scale to multiple replicas**, this check is a race condition —
two drivers could both hit "accept" on two different server instances at
nearly the same moment, and both could see `status: requested` before
either one's event is processed. Real systems solve this with an atomic
external lock (e.g. Redis `SETNX rideId driverId`, or a database
row-level lock) so only one instance's write wins, and the loser gets
told the ride is taken. This demo intentionally keeps that out to stay
focused on the Kafka pub/sub pattern — see "Notes for production" below
if you want to add it.

## Project layout

```
docker-compose.yml       # Docker Compose deployment (local dev)
k8s/                     # Kubernetes deployment (see "Deploying on Kubernetes" below)
app/
  server.js       # REST API + Kafka producer/consumer + Socket.IO rooms
  package.json
  Dockerfile
  public/index.html   # Rider / Driver tabs, single page
```

## Running it

```bash
docker compose up -d
docker compose ps        # wait for kafka and zookeeper to be healthy/running
```

Open two browser tabs at `http://localhost:3001`:

**Tab 1 — Rider:**
- Fill in pickup/drop/distance, click **Request ride**
- You'll see the ride card with status `requested`, then live-update to
  `accepted` once a driver takes it, then `completed`

**Tab 2 — Driver:**
- Switch to the **Driver** tab, click **Go online**
- The ride you requested in Tab 1 appears under "Open ride requests"
- Click **Accept** — it moves to "Your active ride"
- Click **Complete ride** when done — both tabs update live

**Try the race condition yourself:** open a third tab as a second driver,
go online, and try to accept the same ride as Tab 2 at the same time —
whichever request the server processes first wins; the other gets a
"ride is no longer available" alert.

## REST API reference

```
POST /api/rides                    { riderId, riderName, pickup, drop, distanceKm }
GET  /api/rides/open               -> list of open (unaccepted) ride requests
GET  /api/rides/:id                -> one ride's current state
POST /api/rides/:id/accept         { driverId, driverName }
POST /api/rides/:id/complete       (no body needed)
POST /api/rides/:id/cancel         (no body needed)
```

Try it with `curl`:
```bash
curl -X POST localhost:3001/api/rides \
  -H "Content-Type: application/json" \
  -d '{"riderId":"r1","riderName":"Alice","pickup":"MG Road","drop":"Airport","distanceKm":12}'

curl localhost:3001/api/rides/open
```

## Fare calculation

`fare = ₹40 base + ₹12 × distanceKm`, rounded to the nearest rupee. This
is a flat estimate that's also used as the final fare in this demo — a
real system would recompute based on actual route/time and could add
surge pricing, waiting time, etc.

## See the raw events (optional)

Open `http://localhost:8081` for a Kafka UI to browse the `ride-events`
topic directly and watch each `REQUESTED`/`ACCEPTED`/`COMPLETED` message
as it lands.

## Notes for production

- **Race-safe accept**: add a Redis (or DB) atomic lock as described
  above before scaling past one replica.
- **Persistence**: rides live in memory only, lost on restart — swap in
  Postgres/Mongo for durable ride history.
- **Driver location/matching**: this demo broadcasts every ride request
  to every driver regardless of distance. A real system would filter by
  proximity (geospatial query) before notifying drivers.
- **Sticky sessions**: already handled in the Kubernetes manifests below
  (`sessionAffinity: ClientIP`) — needed because Socket.IO's handshake
  must land on the same pod every time for one client.
- **Auth**: `riderId`/`driverId` are just random client-side strings here
  — add real authentication before this goes anywhere near production.

---

## Deploying on Kubernetes

See **`DEPLOY.md`** for the full step-by-step guide, including exact
commands for both CRI-O and containerd runtimes, and what to do every
time you change the app code and need to redeploy.

The `k8s/` folder has the full deployment, with every environmental fix
already applied (from hands-on debugging of an earlier sibling project —
see `enableServiceLinks`, the Kafka advertised-listener setting, sticky
sessions, and image-registry prefixing below).

### 0. One-time cluster prerequisites

```bash
chmod +x setup-prerequisites.sh
./setup-prerequisites.sh
```
This installs a default StorageClass if none exists (needed for
Zookeeper/Kafka's PersistentVolumeClaims), warns you if the node's
container-registry resolution needs fixing (RHEL/CentOS — see below),
and tells you which image-loading method applies to your cluster's
runtime.

**Docker Hub pulls must resolve correctly.** On RHEL/CentOS nodes, if
`/etc/containers/registries.conf`'s `unqualified-search-registries` lists
`registry.redhat.io` before `docker.io`, unprefixed image names get
rewritten to Red Hat's registry and fail with a "login to Red Hat
Registry" error. Every image in these manifests is already prefixed with
`docker.io/` as protection against this, but the script checks the node
config too.

### 1. Build the ride-app image and load it into the cluster

```bash
cd app
docker build -t mini-ride-app:1.0 .
```

How you get this image visible to the cluster depends on your container
runtime (`kubectl get node -o wide`, `CONTAINER-RUNTIME` column):

- **CRI-O**:
  ```bash
  skopeo copy docker-daemon:mini-ride-app:1.0 containers-storage:mini-ride-app:1.0
  crictl images | grep mini-ride-app
  ```
- **containerd**:
  ```bash
  docker save mini-ride-app:1.0 -o /tmp/ride-app.tar
  ctr -n k8s.io images import /tmp/ride-app.tar
  ```
- **minikube/kind**: `minikube image load mini-ride-app:1.0` or
  `kind load docker-image mini-ride-app:1.0`
- **Real cluster with a registry**: `docker push` to your registry and
  update the `image:` field in `k8s/04-ride-app.yaml`.

`imagePullPolicy: IfNotPresent` is already set, so once the image is
locally visible to the runtime, Kubernetes won't try to pull it from
anywhere. If you rebuild the image under the same tag later, you must
delete the pods (`kubectl delete pod -l app=ride-app`) to pick up the new
content.

### 2. Deploy everything

```bash
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/01-zookeeper.yaml
kubectl apply -f k8s/02-kafka.yaml

# wait for zookeeper to be Running (kafka depends on it and will
# crash-loop harmlessly until zookeeper answers)
kubectl -n ride-hailing get pod -w
```

Once `zookeeper-0` shows `1/1 Running`, continue:
```bash
kubectl apply -f k8s/03-create-topic-job.yaml
kubectl apply -f k8s/04-ride-app.yaml
kubectl -n ride-hailing get pods,svc,statefulset,job
```

### 3. Access it

From any machine that can reach the node's IP (this uses NodePort, not
`kubectl port-forward` — port-forward only binds to `localhost` on the
machine running `kubectl`, so it won't work from a separate laptop/phone):
```
http://<node-ip>:30081
```

### Fixes already applied in these manifests (and why)

| Fix | Why it's needed |
|---|---|
| `enableServiceLinks: false` on every pod | Without it, Kubernetes auto-injects `KAFKA_PORT=tcp://...` because the Kafka Service is named `kafka` — Confluent's image mistakes this for deprecated manual config and refuses to start |
| `KAFKA_ADVERTISED_LISTENERS` uses short name `kafka:9092`, not the FQDN | The FQDN `kafka.ride-hailing.svc.cluster.local` has only 4 dots — below Kubernetes' `ndots:5` threshold — causing wasteful/unreliable DNS search-domain expansion that can exceed the Kafka client's connection timeout |
| `sessionAffinity: ClientIP` on both ride-app Services | Socket.IO's handshake needs every request from one client to land on the same pod; without this, Kubernetes' default random load balancing can split a single client's handshake across pods and break the connection |
| `docker.io/` prefix on every image | On RHEL/CentOS nodes, unprefixed image names can get resolved against Red Hat's registry first and fail — an explicit prefix avoids the ambiguity regardless of node config |
| `node:20-bookworm-slim` base image (not `-alpine`) | Alpine's musl libc has well-documented intermittent DNS resolution issues under Kubernetes/CoreDNS |

See the standalone `kafka-k8s-complete-troubleshooting-guide.md` (from
the sibling mini-WhatsApp project) for the full diagnosis story behind
each of these — the entrypoint-tracing technique described there applies
to debugging any similar container crash-loop, not just these specific
bugs.
