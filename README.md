# BUSCO-tracker

Automated BUSCO analysis for eukaryotic genome annotations. 

## Status

**Last updated:** 2026-03-04T19:54:59Z

| Metric | Count | Percentage |
|--------|-------|------------|
| **Total annotations** | 14,800 | 100% |
| **Has BUSCO values** | 14,104 | 95.3% |
| **Pending/Retry** | 0 | 0.0% |
| **Given up** | 696 | 4.7% |

![BUSCO eukaryota results](assets/figures/BUSCO_euk_1k.png)
*Busco values for 1k randomly sampled annotations. Completness is based on eukaryotic BUSCO genes (129). Quality value thresholds are arbitrary*


Find all annotations on [annotrieve](https://genome.crg.es/annotrieve/) and and interact with them using [annocli](https://github.com/apollo994/annocli) 


```
annocli summary --annotation-ids 10cc9bb02de0e989a4710ed0a162be4c
```
