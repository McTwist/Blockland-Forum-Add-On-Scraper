# Blockland Forum Add-On Scraper

A script to scrape the Add-Ons from [Blockland forum](https://forum.blockland.us/). It is designed to be optimal and friendly to all sites that it connects to.

## DISCLAIMER

**I take no responsibility for anyone being banned for using this script.**

## Install

The script uses two modules that can be installed through pip. Those are [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/) and [Requests](http://docs.python-requests.org/en/master/).

```
beautifulsoup4
requests
```

## Run

Use your favorite Python 3 version installed.

```
python blscraper.py
```

This will take around 4 to 8 hours to scrape all valid Add-Ons from the forum Add-On forum board and put them into an SQLite file. Keep in mind that it will only download the Add-Ons if you specify so, else it will only save their links.

The data within the database can then be accessed normally and used to in any way desired.

It can also be run with parameters to customize it further.

```
python blscraper.py [-j threads] [-t timeout] [-r retries] [-d path] [-b sleep] [-v[v[v]]] [--db database_file] [urls [...]]
```

```
threads = Amount of threads to use at most. Default: Core count
timeout = How long time to wait until giving up on a link. Default: 10
retries = How many times to try the link before giving up. Default: 1
path = Path to where to download the files. Default: None
sleep = How long you will wait between each call to a domain. Default: 5,10
database_file = The path to store the database to. Default: blforum.sqlite
-vvv  = Amount of verbosity. More v, the more verbose. Default: 0
urls = All the urls to check against. Default: Link to Blockland Forum Add-On Board
```
