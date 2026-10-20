#!/usr/bin/awk -f
# Cleans a GFF3 before protein extraction with gffread.
#
# Usage: awk -f clean_gff.awk annotation.gff3 > clean.gff3
#
# - Drops features on MT/chrM and features with '?' strand
# - TOGA2: escapes commas in column 9. TOGA2 transcript IDs contain commas
#   (e.g. ...#2269,8703$1), which GFF3 reads as multiple parents and which
#   crash gffread
# - gene -> CDS without a transcript level (common in GenBank GFFs): adds one
#   mRNA per CDS ID. Without it gffread can crash, and
#   longest_transcript_per_gene.awk finds no transcripts and outputs nothing
# - Drops CDS attached directly to a pseudogene. Some GenBank GFFs also give
#   such pseudogenes an mRNA, and the mix crashes gffread
#
# Assumes gene and pseudogene lines precede their CDS lines, as in NCBI GFFs.

BEGIN { FS = OFS = "\t" }

/^#/ { print; next }

$1 == "MT" || $1 == "chrM" || $7 == "?" { next }

$2 == "TOGA2" { gsub(/,/, "%2C", $9) }

$3 == "pseudogene" && match($9, /ID=[^;]+/) { pseudogene[substr($9, RSTART + 3, RLENGTH - 3)] = 1 }

$3 == "CDS" && match($9, /Parent=[^;]+/) && (substr($9, RSTART + 7, RLENGTH - 7) in pseudogene) { next }

$3 == "gene" && match($9, /ID=[^;]+/) {
    gene_span[substr($9, RSTART + 3, RLENGTH - 3)] = $4 OFS $5
}

$3 == "CDS" && match($9, /Parent=[^;]+/) && (substr($9, RSTART + 7, RLENGTH - 7) in gene_span) {
    parent = substr($9, RSTART + 7, RLENGTH - 7)
    tx = match($9, /ID=[^;]+/) ? "tx-" substr($9, RSTART + 3, RLENGTH - 3) : "tx-" parent
    if (!(tx in added)) {
        added[tx] = 1
        print $1, $2, "mRNA", gene_span[parent], ".", $7, ".", "ID=" tx ";Parent=" parent
    }
    sub(/Parent=[^;]+/, "Parent=" tx, $9)
}

{ print }
