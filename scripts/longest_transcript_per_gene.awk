#!/usr/bin/awk -f
# Selects the longest protein sequence per gene from a protein FASTA file.
# Transcript-to-gene relationships are read from a GFF3 annotation file.
#
# Usage: awk -f longest_transcript_per_gene.awk annotation.gff3 proteins.faa > longest_proteins.faa
#
# Pass 1 (GFF3): builds transcript_parent[transcript_id] = gene_id
# Pass 2 (FAA):  stores sequences and counts amino acids per transcript
# END:           builds longest_transcript[gene_id] = transcript_id
#                and writes the output FASTA

FNR == 1 { file_num++ }

# --- Pass 1: GFF3 — build transcript → parent (gene) map ---
file_num == 1 {
    if (/^#/)                                  next
    if ($3 != "mRNA" && $3 != "transcript")    next

    id = ""; parent = ""
    n = split($9, attrs, ";")
    for (i = 1; i <= n; i++) {
        if (attrs[i] ~ /^ID=/)     id     = substr(attrs[i], 4)
        if (attrs[i] ~ /^Parent=/) parent = substr(attrs[i], 8)
    }
    if (id != "" && parent != "") transcript_parent[id] = parent
    next
}

# --- Pass 2: FAA — store sequences and count amino acids ---
file_num == 2 {
    if (/^>/) {
        current         = substr($1, 2)    # strip leading '>'
        fasta_header[current] = $0
        aa_count[current]     = 0
        fasta_seq[current]    = ""
    } else {
        gsub(/[[:space:]]/, "")            # strip any whitespace within sequence
        aa_count[current] += length($0)
        fasta_seq[current]  = fasta_seq[current] $0
    }
    next
}

END {
    # Build longest_transcript[gene_id] = transcript_id with most amino acids
    for (tid in transcript_parent) {
        gid = transcript_parent[tid]
        if (!(gid in longest_transcript) || aa_count[tid] > aa_count[longest_transcript[gid]])
            longest_transcript[gid] = tid
    }

    # Output one FASTA record per gene (the longest transcript)
    for (gid in longest_transcript) {
        tid = longest_transcript[gid]
        if (tid in fasta_header) {
            print fasta_header[tid]
            print fasta_seq[tid]
        }
    }
}
