#!/usr/bin/env bash
# Synchro quotidienne des données RNCP (France Compétences) dans les tables
# minuscules. Journalise horodatage + code de sortie pour repérer un échec.
cd /home/baal/leha || exit 1
LOG=/home/baal/leha/sync-cron.log
{
  echo "===== $(date -Is) : debut sync RNCP ====="
  ./venv/bin/python sync_lowercase.py
  code=$?
  echo "$(date -Is) : fin sync RNCP (exit $code)"
} >> "$LOG" 2>&1
exit ${code:-0}
