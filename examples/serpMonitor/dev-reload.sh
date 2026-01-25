# make sure Docker points to minikube
eval $(minikube docker-env)

TAG=$(date +%s)
docker build -t serpmonitor:${TAG} -f ./Dockerfile ../../

helm upgrade serp-monitor ../../charts/f4a-runner --set image=serpmonitor:$TAG

# wait for rollout and pod to be ready, then tail logs
kubectl rollout status deployment/serp-monitor
kubectl wait --for=condition=Ready pod -l app=f4a-runner --timeout=5s
kubectl logs -f -l app=f4a-runner