#!/usr/bin/env python
"""
Request all the articles currently popular on Hacker News
(news.ycombinator.com), clean them up with BeautifulSoup, and save the results
to MongoDB.
"""
import feedparser
import requests
import pymongo
import logging

from bs4 import BeautifulSoup

mongo = pymongo.Connection()
db = mongo["hackernews"]

logging.basicConfig(
    format='[%(asctime)s %(module)s %(levelname)s] %(message)s',
    level=logging.DEBUG,
)
logger = logging.getLogger(__name__)


hackernews = "http://news.ycombinator.com/rss"


def upsert(entry):
    """Ensures object exists and returns it in full."""

    record = db.articles.find_one({"link": entry["link"]})
    if not record:
        logger.debug("No record found for %s. Inserting one..." %
                     entry["link"])
        new_id = db.articles.insert(entry, safe=True)
        record = db.articles.find_one({"_id": new_id})
    else:
        logger.debug("Record found for %s." % entry["link"])
    url = record.get("link")
    if url:
        populate(url)
    else:
        logger.error("Unexpected: no url for %s" % record)
    return record


def populate(url):
    """Save to MongoDB."""

    record = db.articles.find_one({"link": url})
    if record.get("text"):
        logger.info("Already populated %s." % url)
        return
    logger.debug("Grabbing %s" % url)
    text = grab(url)
    if not text:
        logger.warning("Timeout for %s." % url)
        return
    logger.debug("Saving new text into the database...")
    record["text"] = text
    db.articles.save(record)


def process(soup):
    """Logic around preprocessing the html."""

    return soup.get_text(strip=True)


def grab(url):
    """Logic around requesting the URL."""

    try:
        response = requests.get(url)
    except requests.exceptions.ConnectionError:
        return
    if response.status_code != 200:
        logger.error("Unexpected HTTP response code %(code)s for %(url)s" %
                     dict(
                         code=response.status_code,
                         url=url,
                     ))
    soup = BeautifulSoup(response.content)
    return process(soup)


def run():
    """Main method to run the script."""

    logger.info("Parsing %s..." % hackernews)
    rss = feedparser.parse(hackernews)
    entries = rss["entries"]
    logger.info("Upserting %d entries..." % len(entries))
    for entry in entries:
        upsert(entry)

if __name__ == "__main__":
    run()
