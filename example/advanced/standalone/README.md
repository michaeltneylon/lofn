# GSNAP Alignment on Spark Standalone

## Data

For this example, we have used a volume we mount at /data, so either change the paths or place data in the same location.

### Get Sequence Data

Download sample data, paired-end fastq files. Here we used study GSE67835 (GEO ID) from the SRA (SRR1975008)
 and placed the files in `/data/rnaseq/GSE67835/`

Use the SRA Toolkit to download public data sets from the Sequence Read Archive (SRA)

```
cd $HOME
wget --output-document sratoolkit.tar.gz http://ftp-trace.ncbi.nlm.nih.gov/sra/sdk/current/sratoolkit.current-ubuntu64.tar.gz
tar -vxzf sratoolkit.tar.gz
export PATH=$PATH:$HOME/sratoolkit.2.8.2-1-ubuntu64/bin
mkdir -p /data/rnaseq/GSE67835
cd /data/rnaseq/GSE67835
prefetch SRR1975008
fastq-dump --split-files SRR1975008
```

### Get Reference Genome

Follow [GMAP guidelines](http://research-pub.gene.com/gmap/src/README) to build a reference genome for GSNAP.

In this example we have placed the reference genome within `/data/gmap/`

## Run

`spark-submit alignment.py`
