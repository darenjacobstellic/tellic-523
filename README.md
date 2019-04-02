# [Tellic-523](https://tellic.atlassian.net/browse/TELLIC-523)

## ETL the OMIM, Gene Ontology, +1Data
Download data from source and put the data in BQ.
```
* OMIM As this requires a license, so Eric said to hold off on that.
* Gene Ontology
** Getting the human gaf files from [here](http://current.geneontology.org/annotations/index.html)
** Uploading the extraacted tar files to [GCP](https://console.cloud.google.com/storage/browser/tellic-dev/geneontology/)
* +1Data = [GeneRIF](ftp://ftp.ncbi.nih.gov/gene/GeneRIF/)
```
### Completed:
* Code to download / upload Gene Ontology files to GCS

### Todo:
* ETL Gene Ontology files
* Download and ETL +1Data


#### Installation and running notes:
```
Run from GCP VM with permissions to access GCS
```

