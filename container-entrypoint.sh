#!/bin/sh
set -eu

if [ -n "${PREFECT_API_PRIVATE_IP:-}" ]; then
    api_authority=${PREFECT_API_URL#*://}
    api_host=${api_authority%%/*}
    api_host=${api_host%%:*}

    if [ -z "$api_host" ]; then
        echo "PREFECT_API_URL does not contain a hostname" >&2
        exit 1
    fi

    if ! printf '%s\n' "$PREFECT_API_PRIVATE_IP" | grep -Eq '^[0-9]{1,3}(\.[0-9]{1,3}){3}$'; then
        echo "PREFECT_API_PRIVATE_IP must be an IPv4 address" >&2
        exit 1
    fi

    # /etc/hosts is usually bind-mounted by the container runtime, so it can't
    # be replaced via rename (as `sed -i` does); rewrite it in place instead.
    if ! hosts=$(awk -v host="$api_host" '
        { for (i = 2; i <= NF; i++) if ($i == host) next }
        { print }
    ' /etc/hosts) || ! {
        printf '%s\n' "$hosts"
        printf '%s\t%s\n' "$PREFECT_API_PRIVATE_IP" "$api_host"
    } > /etc/hosts; then
        echo "Unable to update /etc/hosts for $api_host" >&2
        exit 1
    fi
    echo "Mapped $api_host to $PREFECT_API_PRIVATE_IP"
fi

exec "$@"