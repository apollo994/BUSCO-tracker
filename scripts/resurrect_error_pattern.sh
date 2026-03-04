#!/usr/bin/env bash
set -euo pipefail

# This is used to send back to retry annotation matching a given pattern.
# The pattern can be the error message, annotation id or anything in rety
#
# It writes the entries from giveup to retry and remove them from giveup

giveup=$1
retry=$2
pattern=$3

grep "$pattern" "$giveup" | awk 'NR % 2' >> "$retry"
cp "$giveup" "$giveup.tmp"
grep -v "$pattern" "$giveup.tmp" > "$giveup"
rm "$giveup.tmp"
