# Master Thesis: Finding a Needle in a Stack of Logs

This is our implementation of Brauckhoff et. als article Anomaly extraction in backbone networks using association rules. The main function is run_all.py

The two main files used are 
- __detector.py__: Main part of algorithm used to process data
- __elasticquery.py__: Used to fetch data from ElasticSearch server
- __run_all.py__: Runs our program

A config file has to be created with username and password to the ElasticSearch server for usage.
