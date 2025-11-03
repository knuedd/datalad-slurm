#!/usr/bin/env bash
set -euo pipefail
install -d -o munge -g munge -m 0755 /run/munge

# If /etc/munge is writable, chown it to munge. If it's a read-only/shared mount,
# add the munge user to the group owning the mount so it can read group-readable files.
if [ -w /etc/munge ] 2>/dev/null; then
  chown -R munge:munge /etc/munge /run/munge /var/lib/munge 2>/dev/null || true
else
  if [ -d /etc/munge ]; then
    gid=$(stat -c '%g' /etc/munge)
    # find existing group name for that gid, or create one
    grp=$(getent group "$gid" | cut -d: -f1 || true)
    if [ -z "$grp" ]; then
      grp="shared_munge_${gid}"
      groupadd -g "$gid" "$grp" 2>/dev/null || true
    fi
    # add munge to that group so it can read group-readable key files
    usermod -a -G "$grp" munge || true

    # warn if key is not group-readable — fix on host
    if [ -f /etc/munge/munge.key ]; then
      if [ ! -r /etc/munge/munge.key ] || [ "$(stat -c '%a' /etc/munge/munge.key)" -lt 440 ]; then
        echo "WARNING: /etc/munge/munge.key is not group-readable. On the host run: chmod 0440 /path/to/munge.key" >&2
      fi
    else
      echo "WARNING: /etc/munge/munge.key missing in mount; container cannot create it on a read-only mount." >&2
    fi
  fi
fi

# Start MUNGE as 'munge' via gosu, prefer syslog to avoid touching log dir
exec gosu munge /usr/sbin/munged --verbose --syslog &
sleep 1

# Optional sanity: see the cluster
sinfo || true

# Create a DataLad dataset on the shared /data volume
ds=/data/ds-hello
if [ ! -d "$ds/.git" ]; then
  datalad create -c text2git "$ds"
fi
cd "$ds"

# Hello world job script
cat > job.sh <<'EOF'
#!/bin/bash
#SBATCH --job-name=hello
#SBATCH --output=slurm-1.out
echo "Hello world" >> slurm-1.out
EOF
chmod +x job.sh
datalad save -m "Add job script"

# sleep infinity & wait

# Schedule via the DataLad extension wrapping sbatch
datalad slurm-schedule -o slurm-1.out sbatch ./job.sh


# Poll until the job is no longer in the queue
while squeue -h | grep -q .; do
  sleep 2
done

# Capture open job list
output=$(datalad slurm-finish --list-open-jobs)

echo "$output"

# Verify that there is exactly one COMPLETED job
if echo "$output" | grep -Eq '^[[:space:]]*1[[:space:]]+COMPLETED'; then
  echo "Job completed successfully!"
else
  echo "Unexpected job status:"
  exit 1
fi