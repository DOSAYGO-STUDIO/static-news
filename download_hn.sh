#!/bin/bash

# ==========================================
# 0. PROJECT SELECTION
# ==========================================
if [ -n "${PROJECT_ID:-}" ]; then
    echo "Using PROJECT_ID from environment: $PROJECT_ID"
elif [ -n "${GCLOUD_PROJECT:-}" ]; then
    PROJECT_ID="${GCLOUD_PROJECT}"
    echo "Using GCLOUD_PROJECT: $PROJECT_ID"
else
    current_project="$(gcloud config get-value project 2>/dev/null || true)"
    if [ -n "$current_project" ] && [ "$current_project" != "(unset)" ]; then
        PROJECT_ID="$current_project"
        echo "Using gcloud configured project: $PROJECT_ID"
    else
        echo "Fetching your Google Cloud projects..."
        projects_list=$(gcloud projects list --format="value(projectId)")

        if [ -z "$projects_list" ]; then
            echo "No projects found. Set GCLOUD_PROJECT or run 'gcloud auth login' first."
            exit 1
        fi

        projects=($projects_list)
        count=${#projects[@]}

        echo "Available Projects:"
        for i in "${!projects[@]}"; do
            echo "[$i] ${projects[$i]}"
        done

        echo ""
        read -p "Select a project number [0-$(($count-1))]: " selection

        if ! [[ "$selection" =~ ^[0-9]+$ ]] || [ "$selection" -ge "$count" ] || [ "$selection" -lt 0 ]; then
            echo "Invalid selection."
            exit 1
        fi

        PROJECT_ID="${projects[$selection]}"
        echo "Selected Project: $PROJECT_ID"
    fi
fi

# ==========================================
# 0.5 CHECK FOR ORPHAN BUCKETS
# ==========================================
echo ""
echo "Checking for orphan buckets from interrupted runs..."
ORPHANS=$(gsutil ls 2>/dev/null | grep "hn-prime-export" || true)
if [ -n "$ORPHANS" ]; then
    echo "Found orphan bucket(s):"
    echo "$ORPHANS"
    read -p "Delete these buckets? [y/N]: " confirm
    if [[ "$confirm" =~ ^[Yy]$ ]]; then
        for bucket in $ORPHANS; do
            echo "Deleting $bucket..."
            gsutil rm -r "$bucket" >/dev/null 2>&1 || true
        done
        echo "Cleanup complete."
    fi
else
    echo "No orphan buckets found."
fi

# ==========================================
# 1. SETUP & API CHECK
# ==========================================
BUCKET_NAME="hn-prime-export-$(date +%s)"
REGION="US"
LOCAL_DIR="./data/raw"
LOG_FILE="bq_export.log"

echo ""
echo "[1/4] Ensuring BigQuery API is on..."
gcloud services enable bigquery.googleapis.com --project "$PROJECT_ID" > /dev/null 2>&1
echo "      API Ready."

echo ""
echo "[2/4] Creating temporary bucket gs://$BUCKET_NAME..."
gsutil mb -p $PROJECT_ID -l $REGION "gs://$BUCKET_NAME" > /dev/null

# ==========================================
# 2. THE BIG QUERY (BACKGROUNDED)
# ==========================================
echo ""
echo "[3/4] Launching BigQuery Export Job..."

# Run this in the background (&) so we can monitor progress.
bq query \
--project_id="$PROJECT_ID" \
--location="$REGION" \
--use_legacy_sql=false \
"
EXPORT DATA OPTIONS(
  uri='gs://$BUCKET_NAME/hn-*.json.gz',
  format='JSON',
  compression='GZIP',
  overwrite=true
) AS
SELECT id, title, \`by\`, score, time, \`type\`, text, url, parent
FROM \`bigquery-public-data.hacker_news.full\`
WHERE deleted IS NULL OR deleted = false
" > "$LOG_FILE" 2>&1 &

BQ_PID=$!
echo "      Job started (PID $BQ_PID). Logging to $LOG_FILE."

# ==========================================
# 3. THE LIVE MONITOR
# ==========================================
echo "      Waiting for data to materialize... (This takes 5-10 mins)"
echo "      I will check the bucket size every 10 seconds."
echo ""

spinner="/-\|"
start_time=$(date +%s)

while kill -0 $BQ_PID 2>/dev/null; do
    current_time=$(date +%s)
    elapsed=$((current_time - start_time))
    
    SIZE_BYTES=$(gsutil du -s "gs://$BUCKET_NAME" 2>/dev/null | awk '{print $1}')
    if [ -z "$SIZE_BYTES" ]; then SIZE_BYTES=0; fi
    SIZE_MB=$((SIZE_BYTES / 1024 / 1024))
    
    if [ "$SIZE_MB" -eq 0 ]; then
        STATUS="Phase 1: Querying & Sorting (BigQuery is thinking...)"
    else
        STATUS="Phase 2: Exporting... ($SIZE_MB MB written)"
    fi
    
    for i in {0..3}; do
        printf "\r[${spinner:$i:1}] Time: ${elapsed}s | $STATUS"
        sleep 0.5
    done
done

echo "" # New line after loop finishes

# ==========================================
# 4. CHECK RESULT & DOWNLOAD
# ==========================================
wait $BQ_PID
EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
    echo "❌ ERROR: The BigQuery job failed."
    cat "$LOG_FILE"
    echo "Cleaning up bucket..."
    gsutil rm -r "gs://$BUCKET_NAME" >/dev/null 2>&1
    exit 1
fi

echo "✅ Job Complete!"

echo ""
echo "[4/4] Downloading files to $LOCAL_DIR..."
mkdir -p "$LOCAL_DIR"

# FIX: Added -o "GSUtil:parallel_process_count=1" to prevent Python crash on macOS
gsutil -m -o "GSUtil:parallel_process_count=1" cp "gs://$BUCKET_NAME/*.json.gz" "$LOCAL_DIR/"

echo ""
echo "Cleaning up cloud resources..."
gsutil rm -r "gs://$BUCKET_NAME" >/dev/null

echo "SUCCESS. You are ready for 'node etl-hn.js'."
