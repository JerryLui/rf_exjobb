# Master Thesis: Finding a Needle in a Stack of Logs

This is our implementation of Brauckhoff et. als article Anomaly extraction in backbone networks using association rules. A copy of our report is included in this repository as PDF.

The main modules used are 
- __detector.py__: Main part of algorithm used to process data for anomalies
- __elasticquery.py__: Used to fetch data from ElasticSearch server or load file from disk
- __run_all.py__: Runs whole program and extracts data, see this first for basic usage

A config file __settings.py__ has to be created with username and password to the ElasticSearch server for usage as follows.

```python
server = 'https://es-elk-syslog.etc.com:1982'
index = 'index*'
username = 'username'
password = 'password'
```
