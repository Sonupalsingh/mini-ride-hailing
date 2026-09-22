# Deploy Guide — CRI-O aur containerd dono ke liye

Ye document batata hai ki jab bhi `app/server.js` ya `app/public/index.html`
(ya koi bhi app code) change karo, use Kubernetes cluster pe kaise deploy
karna hai — dono common container runtimes (CRI-O aur containerd) ke liye
alag-alag exact commands ke saath.

## Sabse pehle: pata karo aapka cluster kaunsa runtime use karta hai

```bash
kubectl get node -o wide
```

`CONTAINER-RUNTIME` column dekho:
- `cri-o://...` dikhe toh **CRI-O** wala section follow karo
- `containerd://...` dikhe toh **containerd** wala section follow karo

---

## Option A — CRI-O (jaise RHEL/CentOS par kubeadm cluster)

```bash
# 1. Naya image build karo
cd ~/mini-ride-hailing/app
docker build -t mini-ride-app:1.0 .

# 2. CRI-O ke andar import karo (Docker ka build output CRI-O ko khud
#    se dikhta nahi, skopeo se copy karna padta hai)
skopeo copy docker-daemon:mini-ride-app:1.0 containers-storage:mini-ride-app:1.0

# 3. Confirm karo image CRI-O ko dikh raha hai
crictl images | grep mini-ride-app

# 4. Purane pods delete karo taaki naya image pick ho
#    (imagePullPolicy: IfNotPresent hai, isliye same tag ke andar naya
#    content dalne ke baad bhi pod restart zaroori hai)
kubectl -n ride-hailing delete pod -l app=ride-app

# 5. Naye pods ka status dekho
kubectl -n ride-hailing get pod -w
```

Agar `skopeo` install nahi hai:
```bash
dnf install -y skopeo
```

---

## Option B — containerd (jaise minikube/kind ya kai managed clusters)

```bash
# 1. Naya image build karo
cd ~/mini-ride-hailing/app
docker build -t mini-ride-app:1.0 .

# 2. containerd ke andar import karo
docker save mini-ride-app:1.0 -o /tmp/ride-app.tar
ctr -n k8s.io images import /tmp/ride-app.tar

# 3. Confirm karo image containerd ko dikh raha hai
ctr -n k8s.io images ls | grep mini-ride-app

# 4. Purane pods delete karo taaki naya image pick ho
kubectl -n ride-hailing delete pod -l app=ride-app

# 5. Naye pods ka status dekho
kubectl -n ride-hailing get pod -w
```

**minikube/kind use kar rahe ho?** Inme ye 2-step import aur simple ho
jaata hai, ek hi command se:
```bash
# minikube
minikube image load mini-ride-app:1.0

# kind
kind load docker-image mini-ride-app:1.0
```
Fir seedha step 4 (pod delete) pe jao.

---

## Deploy ke baad kya check karo

```bash
# Sab pods Running hain, koi crash-loop toh nahi
kubectl -n ride-hailing get pods

# Kafka aur zookeeper stable hain
kubectl -n ride-hailing get statefulset

# Agar kuch galat lage, logs dekho
kubectl -n ride-hailing logs -l app=ride-app --tail=50
```

Browser me **hard refresh** (`Ctrl+Shift+R`) zaroor karo — purana JavaScript
cache me reh sakta hai, warna aapke naye changes dikhenge hi nahi.

---

## Poora pehli-baar setup (agar cluster bilkul fresh hai)

```bash
cd ~/mini-ride-hailing

# 1. Storage + registry + runtime check — ek hi baar chalana hai
chmod +x setup-prerequisites.sh
./setup-prerequisites.sh

# 2. Image build + import (upar wale Option A ya B se steps 1-3)

# 3. Sab kuch deploy karo, order important hai
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/01-zookeeper.yaml
kubectl apply -f k8s/02-kafka.yaml

# zookeeper Running hone tak wait karo (kafka isके bina crash-loop karega,
# ye normal hai, wait karo)
kubectl -n ride-hailing get pod -w
# Ctrl+C jab zookeeper-0 "1/1 Running" dikhe

kubectl apply -f k8s/03-create-topic-job.yaml
kubectl apply -f k8s/04-ride-app.yaml

kubectl -n ride-hailing get pods,svc,statefulset,job
```

## Access karna (dusre machine se, jaise Windows se)

`kubectl port-forward` sirf VM ke `localhost` pe bind hota hai — dusre
machine se kaam nahi karega. Iske bajaye NodePort use karo:

```bash
kubectl get node -o wide    # INTERNAL-IP dekho
```

Windows/kisi bhi doosre machine ke browser me:
```
http://<node-ip>:30081
```

## Common issues — quick fixes

| Problem | Fix |
|---|---|
| `docker compose` command hi nahi chalta | ye k8s deployment hai, `docker-compose.yml` use hi nahi ho raha — sirf `k8s/` folder use karo |
| Naya code deploy karne ke baad bhi purana behavior dikh raha hai | image dubara build + import + pod delete karna bhool gaye, ya browser cache — hard refresh karo |
| `kafka-0` crash-loop | zookeeper pehle Running hona chahiye — wait karo |
| Naye machine pe pods `Pending` atke rahe | `./setup-prerequisites.sh` chalao — StorageClass missing hoga |
| CRI-O pe `ErrImagePull` "Red Hat Registry" error | `setup-prerequisites.sh` isko detect karega, ya `/etc/containers/registries.conf` check karo |
