#!/usr/bin/env bash
set -euo pipefail

# Logging helpers
log_info() {
  printf "\033[0;34mℹ\033[0m %s\n" "$*"
}

log_success() {
  printf "\033[0;32m✓\033[0m %s\n" "$*"
}

log_warning() {
  printf "\033[1;33m⚠\033[0m %s\n" "$*"
}

log_error() {
  printf "\033[0;31m✗\033[0m %s\n" "$*" >&2
}

WORKFLOW_NAME="Debug Weekly Rebuild Runner"
WORKFLOW_FILE="debug-rebuild.yml"
TMATE_JOB_NAME="tmate debug runner"
MAX_WAIT_SECONDS=120
WAIT_SECONDS_EXPLICIT="false"
TARGET_REF=""
REPO=""
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
PUPPETEER_SCRIPT="$SCRIPT_DIR/get-tmate-ssh.js"

# SSH robustness options applied to the tmate SSH command
SSH_ROBUST="${SSH_ROBUST:-true}"
SSH_SERVER_ALIVE_INTERVAL="${SSH_SERVER_ALIVE_INTERVAL:-30}"
SSH_SERVER_ALIVE_COUNT_MAX="${SSH_SERVER_ALIVE_COUNT_MAX:-4}"
SSH_CONNECT_TIMEOUT="${SSH_CONNECT_TIMEOUT:-15}"
SSH_TCP_KEEPALIVE="${SSH_TCP_KEEPALIVE:-yes}"
SSH_EXTRA_OPTS="${SSH_EXTRA_OPTS:-}"

GET_TMATE_DEBUG="${GET_TMATE_DEBUG:-true}"

augment_ssh_cmd() {
  local cmd="$1"
  [[ "$SSH_ROBUST" != "true" ]] && { echo "$cmd"; return 0; }
  [[ $cmd =~ ^[[:space:]]*ssh[[:space:]]+ ]] || { echo "$cmd"; return 0; }

  local -a opts=()
  grep -Eq '(-o[[:space:]]*ServerAliveInterval=|-oServerAliveInterval=)' <<<"$cmd" || \
    opts+=("-o" "ServerAliveInterval=${SSH_SERVER_ALIVE_INTERVAL}")
  grep -Eq '(-o[[:space:]]*ServerAliveCountMax=|-oServerAliveCountMax=)' <<<"$cmd" || \
    opts+=("-o" "ServerAliveCountMax=${SSH_SERVER_ALIVE_COUNT_MAX}")
  grep -Eq '(-o[[:space:]]*ConnectTimeout=|-oConnectTimeout=)' <<<"$cmd" || \
    opts+=("-o" "ConnectTimeout=${SSH_CONNECT_TIMEOUT}")
  grep -Eq '(-o[[:space:]]*TCPKeepAlive=|-oTCPKeepAlive=)' <<<"$cmd" || \
    opts+=("-o" "TCPKeepAlive=${SSH_TCP_KEEPALIVE}")

  if [[ -n "$SSH_EXTRA_OPTS" ]]; then
    # shellcheck disable=SC2206
    opts+=($SSH_EXTRA_OPTS)
  fi

  [[ ${#opts[@]} -eq 0 ]] && { echo "$cmd"; return 0; }

  local prefix rest
  prefix="$(echo "$cmd" | sed -E 's/^([[:space:]]*ssh)([[:space:]]+).*/\1\2/')"
  rest="$(echo "$cmd" | sed -E 's/^[[:space:]]*ssh[[:space:]]+//')"
  echo "${prefix}${opts[*]} ${rest}"
}

parse_wait_arg() {
  local raw="$1"
  if [[ "$raw" =~ ^[0-9]+$ ]]; then
    [[ "$WAIT_SECONDS_EXPLICIT" == "false" ]] && MAX_WAIT_SECONDS=$((raw * 60))
    return 0
  fi
  if [[ "$raw" =~ ^([0-9]+)s$ ]]; then
    MAX_WAIT_SECONDS="${BASH_REMATCH[1]}"
    WAIT_SECONDS_EXPLICIT="true"
    return 0
  fi
  if [[ "$raw" =~ ^([0-9]+)m$ ]]; then
    local minutes="${BASH_REMATCH[1]}"
    [[ "$WAIT_SECONDS_EXPLICIT" == "false" ]] && MAX_WAIT_SECONDS=$((minutes * 60))
    return 0
  fi
  if [[ "$raw" =~ ^([0-9]+)h$ ]]; then
    local minutes=$((BASH_REMATCH[1] * 60))
    [[ "$WAIT_SECONDS_EXPLICIT" == "false" ]] && MAX_WAIT_SECONDS=$minutes
    return 0
  fi
  return 1
}

usage() {
  cat <<INNER_EOF
Usage: $(basename "$0") [--wait D] [--wait-seconds N] [--ref REF]

Starts or connects to the ${WORKFLOW_NAME} workflow run that publishes a tmate session.

Options:
  -h, --help          Show this message
  -w, --wait D        Max time to find SSH links (minutes by default; suffix s/m/h for seconds/minutes/hours)
      --wait-seconds N  Same as --wait but keeps seconds semantics
      --ref REF        Git ref (branch/sha) to dispatch (defaults to the current branch)
INNER_EOF
  exit 0
}

ensure_gh_cli() {
  if command -v gh &> /dev/null; then
    return
  fi
  log_error "GitHub CLI 'gh' is required. Please install it first."
  exit 1
}

check_gh_auth() {
  if ! gh auth status &> /dev/null; then
    log_error "GitHub CLI is not authenticated"
    log_info "Run: gh auth login"
    exit 1
  fi
}

ensure_jq() {
  if ! command -v jq &> /dev/null; then
    log_error "jq is required to parse GitHub API responses"
    exit 1
  fi
}

ensure_node() {
  if ! command -v node &> /dev/null; then
    log_error "Node.js is required to run the Puppeteer helper"
    exit 1
  fi
}

detect_repo() {
  if command -v gh &> /dev/null; then
    local gh_repo
    gh_repo=$(gh repo view --json nameWithOwner --jq .nameWithOwner 2>/dev/null || true)
    if [[ -n "$gh_repo" ]]; then
      REPO="$gh_repo"
      return
    fi
  fi
  local origin
  origin=$(git config --get remote.origin.url || true)
  if [[ "$origin" == git@github.com:* ]]; then
    origin=${origin#git@github.com:}
  elif [[ "$origin" == https://github.com/* ]]; then
    origin=${origin#https://github.com/}
  fi
  origin=${origin%.git}
  [[ -n "$origin" ]] && REPO="$origin"
}

find_matching_run() {
  local candidate_runs
  candidate_runs=$(gh run list --workflow="$WORKFLOW_FILE" --repo="$REPO" --limit 20 --json databaseId,status --jq '.[] | select(.status=="in_progress" or .status=="queued") | .databaseId' 2>/dev/null || echo "")
  [[ -z "$candidate_runs" ]] && return 1
  while IFS= read -r run_id; do
    [[ -z "$run_id" ]] && continue
    local job_names
    job_names=$(gh api "repos/$REPO/actions/runs/$run_id/jobs" --jq '.jobs[].name' 2>/dev/null || echo "")
    if echo "$job_names" | grep -qF "$TMATE_JOB_NAME"; then
      echo "$run_id"
      return 0
    fi
  done <<< "$candidate_runs"
  return 1
}

wait_for_tmate() {
  local run_id="$1"
  local waited=0
  local attempt=0
  local needs_login=false
  while [[ $waited -lt $MAX_WAIT_SECONDS ]]; do
    attempt=$((attempt + 1))
    log_info "Attempt $attempt: checking for tmate SSH links (elapsed ${waited}s)"
    local output
    output=$(node "$PUPPETEER_SCRIPT" "$run_id" "$REPO" 2>&1)
    local exit_code=$?
    if [[ $exit_code -ne 0 ]] && grep -q "Not logged in" <<<"$output"; then
      needs_login=true
      break
    fi
    if [[ $exit_code -eq 0 ]]; then
      local ssh_links_json
      ssh_links_json=$(printf "%s\n" "$output" | grep -v '^\[Puppeteer\]' | jq -e '.' 2>/dev/null || echo "{}")
      if [[ -n "$ssh_links_json" && "$ssh_links_json" != "{}" ]]; then
        printf "%s" "$ssh_links_json"
        return 0
      fi
    fi
    if [[ $waited -gt $((MAX_WAIT_SECONDS - 15)) ]]; then
      break
    fi
    sleep 15
    waited=$((waited + 15))
  done
  if [[ "$needs_login" == "true" ]]; then
    log_warning "GitHub login required. Opening browser to authenticate..."
    HEADLESS=false node "$PUPPETEER_SCRIPT" "$run_id" "$REPO" 2>&1
    log_info "Retrying headless connection after login..."
    local retry_output
    retry_output=$(node "$PUPPETEER_SCRIPT" "$run_id" "$REPO" 2>&1)
    local retry_json
    retry_json=$(printf "%s\n" "$retry_output" | grep -v '^\[Puppeteer\]' | jq -e '.' 2>/dev/null || echo "{}")
    if [[ -n "$retry_json" && "$retry_json" != "{}" ]]; then
      printf "%s" "$retry_json"
      return 0
    fi
  fi
  return 1
}

filter_ssh_links() {
  local raw_json="$1"
  local run_id="$2"
  local jobs_json
  jobs_json=$(gh api "repos/$REPO/actions/runs/$run_id/jobs" --jq '.jobs[] | {name: .name, status: .status}' 2>/dev/null || echo "[]")
  local in_progress
  in_progress=$(printf "%s" "$jobs_json" | jq -r 'select(.status=="in_progress" or .status=="queued") | .name')
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    local name cmd
    name=$(echo "$line" | jq -r '.name')
    cmd=$(echo "$line" | jq -r '.cmd')
    if printf "%s\n" "$in_progress" | grep -qF "$name"; then
      printf '%s\t%s\n' "$name" "$(augment_ssh_cmd "$cmd")"
    fi
  done < <(printf "%s" "$raw_json" | jq -c 'to_entries[] | {name: .key, cmd: .value}')
}

main() {
  local wait_arg=""
  local ref_arg=""
  while [[ $# -gt 0 ]]; do
    case $1 in
      -h|--help)
        usage
        ;;
      -w|--wait)
        wait_arg="$2"
        shift 2
        ;;
      --wait-seconds)
        MAX_WAIT_SECONDS="$2"
        WAIT_SECONDS_EXPLICIT="true"
        shift 2
        ;;
      --ref)
        ref_arg="$2"
        shift 2
        ;;
      *)
        log_error "Unknown option: $1"
        usage
        ;;
    esac
  done
  if [[ -n "$wait_arg" ]]; then
    if ! parse_wait_arg "$wait_arg"; then
      log_error "Invalid wait argument: $wait_arg"
      exit 1
    fi
  fi
  if [[ $MAX_WAIT_SECONDS -lt 15 ]]; then
    log_error "Wait time must be at least 15 seconds"
    exit 1
  fi
  if [[ -n "$ref_arg" ]]; then
    TARGET_REF="$ref_arg"
  else
    TARGET_REF=$(git branch --show-current 2>/dev/null || echo "")
    [[ -z "$TARGET_REF" ]] && TARGET_REF="main"
  fi

  ensure_gh_cli
  ensure_node
  check_gh_auth
  ensure_jq
  detect_repo
  if [[ -z "$REPO" ]]; then
    log_error "Unable to detect repository. Set REPO=owner/name if required."
    exit 1
  fi

  log_info "Target repository: $REPO"
  log_info "Workflow: $WORKFLOW_NAME ($WORKFLOW_FILE)"

  local run_id
  run_id=$(find_matching_run || true)
  if [[ -n "$run_id" ]]; then
    log_info "Reusing existing run: https://github.com/$REPO/actions/runs/$run_id"
  else
    log_info "Starting workflow run for ref $TARGET_REF"
    gh workflow run "$WORKFLOW_FILE" --repo "$REPO" --ref "$TARGET_REF"
    sleep 3
    run_id=$(find_matching_run || true)
    if [[ -z "$run_id" ]]; then
      log_error "Could not find the workflow run after dispatch. Check GitHub Actions."
      exit 1
    fi
  fi

  log_success "Workflow run ID: $run_id"
  log_info "Waiting for tmate session to become available..."

  local ssh_json
  ssh_json=$(wait_for_tmate "$run_id" || true)
  if [[ -z "$ssh_json" || "$ssh_json" == "{}" ]]; then
    log_error "Failed to locate tmate SSH links after ${MAX_WAIT_SECONDS}s"
    exit 1
  fi

  mapfile -t sessions < <(filter_ssh_links "$ssh_json" "$run_id")
  if [[ ${#sessions[@]} -eq 0 ]]; then
    log_error "No SSH sessions are currently in progress. Verify the workflow logs."
    exit 1
  fi

  declare -a session_names=()
  declare -a session_cmds=()
  local idx=1
  for entry in "${sessions[@]}"; do
    local name="${entry%%$'\t'*}"
    local cmd="${entry#*$'\t'}"
    if [[ -z "$name" || -z "$cmd" ]]; then
      continue
    fi
    session_names+=("$name")
    session_cmds+=("$cmd")
    printf "  %d) %s\n" "$idx" "$name"
    printf "     → %s\n\n" "$cmd"
    idx=$((idx + 1))
  done

  if [[ ${#session_names[@]} -eq 0 ]]; then
    log_error "Failed to parse available SSH sessions"
    exit 1
  fi

  read -rp "Select session to connect [1-${#session_names[@]}] or 'q' to quit: " selection
  if [[ "$selection" == "q" ]]; then
    log_info "Workflow available at: https://github.com/$REPO/actions/runs/$run_id"
    exit 0
  fi
  if [[ ! "$selection" =~ ^[0-9]+$ ]] || ((selection < 1 || selection > ${#session_names[@]})); then
    log_error "Invalid selection"
    exit 1
  fi

  local selected_idx=$((selection - 1))
  local selected_cmd=${session_cmds[$selected_idx]}
  local selected_name=${session_names[$selected_idx]}

  log_success "Connecting to: $selected_name"
  log_info "Run 'exit' when finished, then rerun this script to reconnect"
  eval "$selected_cmd"
}

main "$@"
