# BUSCO-tracker

Automated BUSCO analysis for eukaryotic genome annotations. 
See all results [here](BUSCO/eukaryota_odb12/BUSCO.tsv).

## Status

**Last updated:** 2026-04-07T07:59:29Z

| Metric | Count | Percentage |
|--------|-------|------------|
| **Total annotations** | 15148 | 100% |
| **Has BUSCO values** | 14446 | 95.4% |
| **Pending/Retry** | 0 | 0.0% |
| **Given up** | 702 | 4.6% |

![BUSCO eukaryota results](assets/figures/BUSCO_euk_1k.png)
*Busco values for 1k randomly sampled annotations. Completness is based on eukaryotic BUSCO genes (129). Quality value thresholds are arbitrary*


Find all annotations on [annotrieve](https://genome.crg.es/annotrieve/) and and interact with them using [annocli](https://github.com/apollo994/annocli) 


```
annocli summary --annotation-ids 10cc9bb02de0e989a4710ed0a162be4c
```
