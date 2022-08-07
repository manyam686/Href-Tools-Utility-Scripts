# HREF Tools Corp. Blog Utility Scripts
These are a collection of python scripts to perform various tasks surrounding
the HREF Tools Corp. blog.

## Requirements
* Set environment variables with database and S3 connection information before
running (development personnel)
* Each script subsection lists required packages

## Article Web Scraping
A python script to utilize web scraping to pull all articles and related information
out of the old blogsite, strip out styling from the old site, put images in S3
and update the html references to them, and insert the information into a
PostgreSQL database.
### Requirements
* beautifulsoup4: 4.10.0
* boto3: 1.21.18
* urllib3: 1.26.8
* psycopg2: 2.9.3

## Article Clean Up
A python script to go through the articles in the PostgreSQL database and clean
up the html markup.
* beautifulsoup4: 4.10.0
* psycopg2: 2.9.3
