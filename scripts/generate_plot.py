import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib.colors import ListedColormap
from matplotlib.font_manager import FontProperties
from matplotlib.patches import Patch

# BUSCO color scheme
BUSCO_COLORS = ["#56B4E9", "#56B4E9", "#3492C7", "#F0E442", "#F04442"]
BUSCO_CATEGORIES = ["complete", "single", "duplicated", "fragmented", "missing"]

BUSCO_COLOR_MAP = {
    "complete": "#56B4E9",
    "single": "#56B4E9",
    "duplicated": "#3492C7",
    "fragmented": "#F0E442",
    "missing": "#F04442",
}


TIER_PALETTE = ["#E0E0E0", "#B0B0B0", "#808080", "#505050", "#202020"]
TIER_ORDER = ["High (≥98%)", "Medium (80-98%)", "Low (50-80%)", "Poor (<50%)"]

TIER_COLOR_MAP = {
    "High (≥98%)": "#F0F0F0",
    "Medium (80-98%)": "#C0C0C0",
    "Low (50-80%)": "#808080",
    "Poor (<50%)": "#404040",
}


def quality_tier(c):
    if c >= 98:
        return "High (≥98%)"
    elif c >= 80:
        return "Medium (80-98%)"
    elif c >= 50:
        return "Low (50-80%)"
    else:
        return "Poor (<50%)"


def main():
    parser = argparse.ArgumentParser(
        description="Generate BUSCO completeness visualization plot"
    )
    parser.add_argument(
        "--busco-tsv",
        type=Path,
        default=Path("BUSCO/eukaryota_odb12/BUSCO.tsv"),
        help="Path to BUSCO results TSV file (default: BUSCO/eukaryota_odb12/BUSCO.tsv)",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=1000,
        help="Number of annotations to sample for plot (default: 1000)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("assets/figures/BUSCO_euk_1k.png"),
        help="Output path for the plot (default: assets/figures/BUSCO_euk_1k.png)",
    )
    args = parser.parse_args()

    sns.set_theme(style="whitegrid")

    # Read BUSCO results
    df = pd.read_csv(args.busco_tsv, sep="\t")

    # Add quality tier
    df["quality"] = df["complete"].apply(quality_tier)

    print(f"{len(df)} annotations with BUSCO results")

    # Sort by completeness (BUSCO complete = single + duplicated, then missing), descending
    # Sample annotations evenly across the sorted dataset
    total_annotations = len(df)
    sample_size = min(args.sample_size, total_annotations)

    if sample_size < total_annotations:
        # Sample every Nth annotation to get requested sample size
        step = total_annotations // sample_size
        df_s = (
            df.iloc[::step,]
            .head(sample_size)
            .sort_values("missing")
            .sort_values("complete", ascending=False)
            .copy()
        )
        print(
            f"Sampling {len(df_s)} annotations (every ~{step} annotation) for visualization"
        )
    else:
        df_s = df.sort_values("missing").sort_values("complete", ascending=False).copy()
        print(f"Using all {len(df_s)} annotations for visualization")

    # Exclude "complete" from the stacks; plot the components instead
    stack_cols = ["single", "duplicated", "fragmented", "missing"]

    # Build plotting DF (one bar per annotation_id)
    plot_df = df_s.set_index("annotation_id")[stack_cols].astype(float)

    # Prepare quality tier data
    tier_to_int = {t: i for i, t in enumerate(TIER_ORDER)}
    q = np.array(df_s["quality"].apply(lambda x: tier_to_int[x]).values)
    heat = q.reshape(1, -1)

    # Calculate tier statistics
    cmap = ListedColormap([TIER_COLOR_MAP[t] for t in TIER_ORDER])
    tier_counts = df_s["quality"].value_counts().reindex(TIER_ORDER, fill_value=0)
    tier_perc = (tier_counts / tier_counts.sum() * 100).round(1)

    # Create layout with two subplots
    fig, (ax1, ax2) = plt.subplots(
        2,
        1,
        sharex=True,
        figsize=(15, 5),
        gridspec_kw={"height_ratios": [8, 1], "hspace": 0.05},
    )

    # Top subplot: BUSCO stacked bars
    plot_df.plot(
        kind="bar",
        stacked=True,
        ax=ax1,
        color=[BUSCO_COLOR_MAP[c] for c in stack_cols],
        edgecolor="none",
        linewidth=0,
    )

    ax1.set_ylabel("BUSCO (%)")
    ax1.set_xlabel("")
    ax1.set_ylim(0, 100)
    ax1.set_xticks([])
    ax1.grid(False)

    # Bottom subplot: Quality tier heatmap strip
    ax2.imshow(
        heat,
        aspect="auto",
        cmap=cmap,
        interpolation="nearest",
        vmin=-0.5,
        vmax=len(TIER_ORDER) - 0.5,
    )
    ax2.set_yticks([])
    ax2.set_xticks([])
    ax2.set_ylabel("Quality", rotation=0, labelpad=25, va="center")

    # Add tier labels with percentages
    n = len(q)
    start = 0
    for tier in TIER_ORDER:
        cnt = int(tier_counts[tier])
        if cnt == 0:
            continue
        end = start + cnt
        center = (start + end - 1) / 2.0

        label = f"{tier_perc[tier]}%\n({tier_counts[tier]})"
        # Text inside the strip if wide enough, else above
        if cnt >= max(3, int(0.03 * n)):
            ax2.text(
                center, 0, label, ha="center", va="center", fontsize=9, color="white"
            )
        else:
            ax2.text(
                center,
                -0.55,
                label,
                ha="center",
                va="bottom",
                fontsize=9,
                clip_on=False,
            )

        start = end

    # Create combined legend
    leg = ax1.get_legend()
    if leg is not None:
        leg.remove()

    busco_handles = [
        Patch(facecolor=BUSCO_COLOR_MAP[c], edgecolor="none") for c in stack_cols
    ]
    tier_handles = [
        Patch(facecolor=TIER_COLOR_MAP[t], edgecolor="none") for t in TIER_ORDER
    ]

    # Section headers (invisible handles)
    hdr_busco = Patch(facecolor="none", edgecolor="none")
    hdr_tier = Patch(facecolor="none", edgecolor="none")
    hdr_empty = Patch(facecolor="none", edgecolor="none")

    handles = [hdr_busco] + busco_handles + [hdr_empty] + [hdr_tier] + tier_handles
    labels = ["BUSCO"] + stack_cols + [""] + ["Quality Tier"] + TIER_ORDER

    ax1.legend(
        handles=handles,
        labels=labels,
        bbox_to_anchor=(1.02, 1),
        loc="upper left",
        frameon=False,
        handlelength=1.2,
        handletextpad=0.6,
    )

    # Make section headers bold and hide their handles
    leg = ax1.get_legend()
    header_idxs = [0, len(stack_cols) + 2]
    for i in header_idxs:
        leg.get_texts()[i].set_fontproperties(FontProperties(weight="bold"))
        leg.legend_handles[i].set_visible(False)

    plt.suptitle(
        f"BUSCO completness of {len(df_s)} annotations (eukaryota_odb12, n={int(df_s["busco_count"].unique()[0])})"
    )

    # Create output directory if it doesn't exist
    args.output.parent.mkdir(parents=True, exist_ok=True)

    # Save the plot
    plt.savefig(args.output, dpi=300, bbox_inches="tight")
    print(f"Plot saved to {args.output}")

    # Optionally show the plot (comment out if running in headless environment)
    # plt.show()


if __name__ == "__main__":
    main()
