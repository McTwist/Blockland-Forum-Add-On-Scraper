import sys
import re
from bs4 import BeautifulSoup
import requests
import concurrent.futures as futures
import sqlite3
from urllib.parse import urlparse
import time
import threading

# Get current core count
# Note: This is interesting: http://stackoverflow.com/a/1006301
def get_core_count():
	try:
		import multiprocessing
		return multiprocessing.cpu_count()
	except (ImportError, NonImplementedError):
		return 1

# Create a dummy object
def create_object(attr):
	return type('Dummy', (object,), attr)

# Handles a databae
class Database:
	@staticmethod
	def connect(file):
		db = Database()
		if db.open(file):
			return db
		else:
			return None

	# Constructor
	def __init__(self):
		self._conn = None

	# Entering with
	def __enter__(self):
		return self
	# Done with with
	def __exit__(self, exc_type, exc_val, exc_tb):
		self.close()

	# Open the database
	def open(self, file):
		if self._conn:
			self.close()

		self._conn = sqlite3.connect(file)
		if not self._conn:
			return False
		# Create the tables needed
		self.create_tables()
		return True

	# Close the database
	def close(self):
		if not self._conn:
			return

		self._conn.close()
		self._conn = None

	# Get cursor
	def _start(self):
		return self._conn.cursor()

	# Close cursor
	def _finish(self):
		self._conn.commit()

	# Create tables if not exist
	def create_tables(self):
		cur = self._start()

		# Create topics
		cur.execute("""CREATE TABLE IF NOT EXISTS topics (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			title TEXT,
			author TEXT,
			author_url TEXT,
			post TEXT,
			url TEXT UNIQUE,
			last_update INTEGER
		)""")
		# Create files
		cur.execute("""CREATE TABLE IF NOT EXISTS files (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT,
			url TEXT UNIQUE,
			topic_id INTEGER NOT NULL
		)""")

		self._finish()

	# Add a new topic
	def add_topic(self, topics):
		cur = self._start()

		if isinstance(topics, ForumTopic):
			topics = [topics]

		# Go through all topics to be added
		for topic in topics:
			if not isinstance(topic, ForumTopic):
				continue

			# Get topic if it exists
			cur.execute("""SELECT id FROM topics WHERE url = ?""", (topic.url,))
			row = cur.fetchone()
			# Update current topic with new info
			if row:
				topic_id = row[0]
				cur.execute("""UPDATE topics SET title = ?, post = ?, last_update = ? WHERE id = ?""", (topic.title, topic.post, topic.timestamp, topic_id))
			# Add new topic
			else:
				cur.execute("""INSERT OR IGNORE INTO topics(title, author, author_url, post, url, last_update)
					VALUES (?, ?, ?, ?, ?, ?)""", (topic.title, topic.author, topic.author_url, topic.post, topic.url, topic.timestamp))
				topic_id = cur.lastrowid
			# Store id in topic for further use
			topic.id = topic_id

		self._finish()

		return topic_id

	# Add a new file
	def add_file(self, files):
		cur = self._start()

		if isinstance(files, ArchiveFile):
			files = [files]

		# Go through all files to be added
		for file in files:
			if not isinstance(file, ArchiveFile):
				continue
			if not file.topic.id:
				continue

			cur.execute("""INSERT OR IGNORE INTO files(name, url, topic_id)
				VALUES (?, ?, ?)""", ( file.name, file.url, file.topic.id ))

		self._finish()

	# Get latest timestamp from topics
	def get_latest_timestamp(self):
		cur = self._start()

		# We only need this value
		cur.execute("SELECT MAX(last_update) FROM topics")
		row = cur.fetchone()
		if row and row[0]:
			return row[0]
		else:
			return 0

# Keeps track of last visited domains and avoids it to bash too much
class AntiDomainBasher:
	domains = dict()
	timer = 1
	lock = threading.RLock()

	# Internal lock holder to store lock and time
	# Keep in mind that you do not modify this. Use it with "with" and you should be fine
	class DomainLock:
		def __init__(self):
			self._lock = threading.RLock()
			self.time = 0

		# Just pass it along
		def __enter__(self):
			return self

		# Unlock it
		def __exit__(self, exc_type, exc_val, exc_tb):
			self._lock.release()

		# Update time
		def update(self):
			self.time = time.time()

		# Manually locking
		def lock(self):
			self._lock.acquire()

	# Wait for your turn on the domain
	# This needs to be in a with statement
	@classmethod
	def wait_for_lock(self, url):
		from time import sleep

		domain = self._get_domain(url)
		# Invalid domain
		if not domain:
			return None
		# Main lock
		with self.lock:
			# Create new lock
			if domain not in self.domains:
				self.domains[domain] = self.DomainLock()
			handle = self.domains[domain]

		# And now for the domain lock
		handle.lock()
		# Wait a little to avoid bashing
		t = max(handle.time - (time.time() - self._get_wait()), 0)
		if t > 0:
			sleep(t + self._get_sleep())
		# Set new time
		handle.update()

		return handle

	# Get domain from url
	@staticmethod
	def _get_domain(url):
		return urlparse(url).netloc

	# Get current minimum wait time
	@classmethod
	def _get_wait(self):
		if isinstance(self.timer, tuple) and len(self.timer) >= 1:
			return self.timer[0]
		else:
			return self.timer

	# Get sleep time
	@classmethod
	def _get_sleep(self):
		from random import randint
		if isinstance(self.timer, tuple) and len(self.timer) >= 2:
			return randint(0, self.timer[1] - self.timer[0])
		else:
			return 0

# Scraping the Blockland forum of its precious Add-Ons
class BlocklandForumScraper:
	# Constructor
	def __init__(self, db):
		self.dbfile = db

		self.settings = create_object({
			"sleep_block": (5, 10),
			"latest_update": 0,
			"one_zip_per_topic": True,
			"timeout": 10,
			"retries": 1
		})

		AntiDomainBasher.timer = self.settings.sleep_block

	# Go through the links
	def process(self, urls):

		with Database.connect(self.dbfile) as db:

			self.settings.latest_update = db.get_latest_timestamp()

			print("Searching through forum...")

			# Prepare urls
			boards = [ForumBoard(self.settings, url) for url in urls]

			# Decorator
			def load(data):
				return data.load()

			visited = set()

			for board in boards:
				visited.add(board.url)

			# Initialize the pool
			with futures.ThreadPoolExecutor(max_workers=get_core_count()) as e:
				future_data = {e.submit(load, board): board for board in boards}

				# Keep going until list is empty
				while len(future_data):
					print("Queued: " + str(len(future_data)))
					for future in futures.as_completed(future_data):
						request = future_data[future]
						del future_data[future]

						try:
							data = future.result()
						except Exception as exc:
							import traceback
							print(traceback.format_exc())
							print("Error: (" + str(type(request)) + ") " + str(exc))
						else:
							# Something bad happened
							if data == False:
								continue
							# Handle types
							if isinstance(request, ForumTopic):
								db.add_topic(request)
							elif isinstance(request, ArchiveFile):
								if data == True:
									db.add_file(request)
								continue

							# Send everything to be loaded
							for item in data:
								# Handle visited pages
								if item.url in visited:
									continue
								# Mark as visited
								visited.add(item.url)

								fut = e.submit(load, item)
								# Add to new one
								future_data[fut] = item

							if len(data):
								# Reload iteration
								break

# Handle forum boards
class ForumBoard:
	# Constructor
	def __init__(self, settings, url):
		self.url = url

		self.settings = settings

	# Load the board
	def load(self):
		with AntiDomainBasher.wait_for_lock(self.url):
			print("Board: " + self.url)
			# Try to get board
			for _ in range(self.settings.retries + 1):
				try:
					resp = requests.get(self.url, timeout=self.settings.timeout)
				except requests.exceptions.Timeout:
					pass # Try again
				except requests.exceptions.RequestException:
					return False
				else:
					break

		soup = BeautifulSoup(resp.text, 'html.parser')

		# Locate all board pages
		boards = [ForumBoard(self.settings, a['href']) for a in soup.find_all(href=self.is_board_link)]
		# Remove the last part
		def remove_by(text):
			return text[:text.find("by")]
		# Locate all topics
		topics = [ForumTopic(self.settings, a.text, a['href'], remove_by(a.parent.find_next_siblings(name='td')[-1].span.text)) for a in soup.find_all(href=self.is_topic_link) if a.parent.name == 'td']
		# Remove old topics
		topics = [topic for topic in topics if topic.timestamp > self.settings.latest_update]
		# Together
		boards.extend(topics)
		return boards

	# Check if link is a topic link
	@staticmethod
	def is_topic_link(href):
		return href and re.compile('\?topic\=([0-9]*)\.0$').search(href)

	# Check if link is a page link
	@staticmethod
	def is_board_link(href):
		return href and re.compile('\?board\=([0-9]*)\.([0-9]*)$').search(href)


# Handle a single topic
class ForumTopic:
	# Constructor
	def __init__(self, settings, title=None, url=None, date=None):
		self.author = ""
		self.author_url = ""
		self.title = title or ""
		self.post = ""
		self.url = url or ""
		self.date = date or ""

		self.zips = []

		self.settings = settings

		# Cache
		self._timestamp = None

	# Load the page and get content
	def load(self):
		with AntiDomainBasher.wait_for_lock(self.url):
			print("Topic: " + self.url)
			# Try to get topic page
			for _ in range(self.settings.retries + 1):
				try:
					resp = requests.get(self.url, timeout=self.settings.timeout)
				except requests.exceptions.Timeout:
					pass # Try again
				except requests.exceptions.RequestException:
					return False
				else:
					break

		soup = BeautifulSoup(resp.text, 'html.parser')

		# Locate author
		def is_profile_link(href):
			return href and re.compile('\?action\=profile;u\=([0-9]*)$').search(href)
		profile = soup.find(href=is_profile_link)
		if profile:
			self.author = profile.text
			self.author_url = profile['href']

		# Locate first post
		post = soup.find(class_="post")
		if post:
			self.post = str(post)
			# Find all links in this post
			files = [ArchiveFile(self.settings, self, link['href']) for link in post.find_all(name='a')]

			return files
		return []

	# Get timestamp from a date
	@property
	def timestamp(self):
		if self._timestamp != None:
			return self._timestamp
		import time
		import datetime

		today = datetime.datetime.now()
		yesterday = today - datetime.timedelta(days=1)

		d = None
		# Just guess it right through
		try:
			# Regular time
			d = datetime.datetime.strptime(self.date, "%B %d, %Y, %I:%M:%S %p")
		except Exception as e:
			try:
				# Today time
				d = datetime.datetime.strptime(self.date, "Today at %I:%M:%S %p")
				d = d.replace(today.year, today.month, today.day)
			except Exception as e:
				try:
					# Yesterday time
					d = datetime.datetime.strptime(self.date, "Yesterday at %I:%M:%S %p")
					d = d.replace(yesterday.year, yesterday.month, yesterday.day) - datetime.timedelta(days=1)
				except Exception as e:
					# Someone broke it
					self._timestamp = 0
		if d:
			self._timestamp = time.mktime(d.timetuple())
		return self._timestamp

# Handles a file
class ArchiveFile:
	# Constructor
	def __init__(self, settings, topic, url=None):
		self.topic = topic
		self.url = url or ""
		self.name = None

		self.settings = settings

	# Guess the filename it could have
	def load(self):
		status = None
		with AntiDomainBasher.wait_for_lock(self.url):
			print("File: " + self.url)
			# Try to get info about the file
			for _ in range(self.settings.retries + 1):
				try:
					resp = requests.head(self.url, allow_redirects=True, timeout=self.settings.timeout)
				except requests.exceptions.Timeout:
					pass # Try again
				except requests.exceptions.RequestException:
					return False
				else:
					headers = resp.headers
					status = resp.status_code
					break
			# Special "head-is-blocked" handling
			if status == 403:
				# We only do this once, to avoid bashing heavy files from their servers
				try:
					resp = requests.get(self.url, timeout=self.settings.timeout)
				except requests.exceptions.Timeout:
					pass # Try again
				except requests.exceptions.RequestException:
					return False
				else:
					headers = resp.headers
					status = resp.status_code

		# Does not exist
		if status != 200:
			return False

		filename = None

		# Might be, do more thorough check
		if 'Content-Disposition' in headers:
			found = re.compile('filename\=\"(((.*)_(.*))\.zip)\"').search(headers['Content-Disposition'])
			if found:
				self.name = found.group(1)
				return True
		file = self.get_url_file(self.url)
		# Check for valid name
		found = re.compile('(((.*)_(.*))\.zip)').search(file)
		if found:
			self.name = found.group(1)
			return True
		# Marked as zip, so add it
		if 'Content-Type' in headers and headers['Content-Type'].lower() == 'application/zip':
			self.name = file
			return True
		
		return False

	# Get file from url
	@staticmethod
	def get_url_file(url):
		return url.split('/')[-1].split('?')[0].split('#')[0]

# Main function
# Start here
def main(argv):

	forum = BlocklandForumScraper(db='blforum.sqlite')
	forum.process(["https://forum.blockland.us/index.php?board=34.0"])

	return 0

if __name__ == "__main__":
	sys.exit(main(sys.argv))
