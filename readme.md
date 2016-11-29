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

Use your favorite Python version installed.

```
python blscraper.py
```

This will take around 4 to 8 hours to scrape all valid Add-Ons from the forum Add-On forum board and put them into an SQLite file. Keep in mind that it wont download the Add-Ons, but rather save their links.

The data within the database can then be accessed normally and used to in any way desired.
