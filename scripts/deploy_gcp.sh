#!/bin/bash
# Create a fresh VM, deploy the repo, and run the clean-room end-to-end flow on the target machine.
set -euo pipefail

PROJECT_ID=${PROJECT_ID:-"trial-homework"}
ZONE=${ZONE:-"asia-southeast1-c"}
MACHINE_TYPE=${MACHINE_TYPE:-"e2-medium"}
INSTANCE_NAME=${INSTANCE_NAME:-"bidding-server-$(date +%s)"}
REPO_URL=${REPO_URL:-"https://github.com/zzqDeco/trial-homework.git"}
REMOTE_DIR=${REMOTE_DIR:-"trial-homework"}
WAIT_TIMEOUT_SECONDS=${WAIT_TIMEOUT_SECONDS:-300}
PROJECTION_TIMEOUT_SECONDS=${PROJECTION_TIMEOUT_SECONDS:-120}

echo "=================================================="
echo "1. Selecting GCP project..."
echo "=================================================="
gcloud config set project $PROJECT_ID

echo "=================================================="
echo "2. Creating GCP VM ($INSTANCE_NAME)..."
echo "=================================================="
gcloud compute instances create $INSTANCE_NAME \
    --project=$PROJECT_ID \
    --zone=$ZONE \
    --machine-type=$MACHINE_TYPE \
    --image-family=debian-12 \
    --image-project=debian-cloud \
    --tags=bidding-server

echo "=================================================="
echo "3. Enabling firewall ports 8080, 8081, 8082..."
echo "=================================================="
# We create a single rule targeting the 'bidding-server' tag
gcloud compute firewall-rules create allow-bidding-server-ports \
    --project=$PROJECT_ID \
    --allow tcp:8080,tcp:8081,tcp:8082 \
    --target-tags=bidding-server \
    || echo "Firewall rule might already exist, continuing..."

echo "Waiting for SSH to become available..."
for i in {1..10}; do
    if gcloud compute ssh $INSTANCE_NAME --zone=$ZONE --project=$PROJECT_ID --command="true" --quiet; then
        break
    fi
    echo "SSH not ready yet, waiting 10 seconds..."
    sleep 10
done

echo "=================================================="
echo "4. Deploying repo and running clean-room E2E..."
echo "=================================================="
REMOTE_COMMAND=$(cat <<EOF
set -euo pipefail

REPO_URL="${REPO_URL}"
REMOTE_DIR="${REMOTE_DIR}"
WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS}"
PROJECTION_TIMEOUT_SECONDS="${PROJECTION_TIMEOUT_SECONDS}"
REPO_PATH="\$HOME/\$REMOTE_DIR"

sudo apt-get update
sudo apt-get install -y git curl docker.io
if ! docker compose version >/dev/null 2>&1 && ! command -v docker-compose >/dev/null 2>&1; then
  sudo apt-get install -y docker-compose-plugin || sudo apt-get install -y docker-compose
fi

sudo systemctl enable --now docker
sudo usermod -aG docker "\$USER"

if [ -d "\$REPO_PATH/.git" ]; then
  cd "\$REPO_PATH"
  git fetch origin
  git checkout main
  git pull --ff-only origin main
else
  git clone "\$REPO_URL" "\$REPO_PATH"
  cd "\$REPO_PATH"
fi

# sg docker makes the fresh VM pick up docker-group access without requiring a new SSH session.
sg docker -c "cd '\$REPO_PATH' && ./scripts/reset_data.sh && WAIT_TIMEOUT_SECONDS=\$WAIT_TIMEOUT_SECONDS PROJECTION_TIMEOUT_SECONDS=\$PROJECTION_TIMEOUT_SECONDS ./scripts/run_e2e.sh"
EOF
)

gcloud compute ssh $INSTANCE_NAME --zone=$ZONE --project=$PROJECT_ID --command="$REMOTE_COMMAND"

echo "=================================================="
echo "5. Retrieving External IP and verifying..."
echo "=================================================="
VM_IP=$(gcloud compute instances describe $INSTANCE_NAME --zone=$ZONE --project=$PROJECT_ID --format='get(networkInterfaces[0].accessConfigs[0].natIP)')

echo "Deployment complete! VM IP is $VM_IP"
echo "Verifying public health endpoints..."
curl -fsS "http://$VM_IP:8080/healthz" >/dev/null
curl -fsS "http://$VM_IP:8082/healthz" >/dev/null

cat <<SUMMARY

Deployment succeeded.

API:       http://$VM_IP:8080
Dashboard: http://$VM_IP:8082

Suggested checks:
  curl http://$VM_IP:8080/healthz
  curl http://$VM_IP:8082/healthz
  open http://$VM_IP:8082
SUMMARY
