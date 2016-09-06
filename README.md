* Second component of Twitter Analytics Application *

      --- Consumer component --- 

A python module to analyze twitter data.

The “Consumer.py” module contains “PyMongo” library and “AFINN-111.txt”. What “PyMongo” is that it provides tools to interact with MongoDB from python. The “AFINN-111.txt” contains about 2500 English words rated for sentiment scores ranged from -5 to 5. This file is used for analyzing sentiment on Twitter data.

This also stores raw data to AWS S3 bucket and analyzed data to MongoDB.
