#!/usr/bin/env bash
set -euo pipefail

cat <<'EOF'
The demo dashboard is session-scoped and needs no cloud reset.
Open a new Streamlit browser session to clear the selected transaction.
No DynamoDB rows, Kinesis records, S3 Tables data, or AWS resources were deleted.
EOF
