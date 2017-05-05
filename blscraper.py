import sys
import re
import getopt
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
			content TEXT,
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
		# Create profiles
		cur.execute("""CREATE TABLE IF NOT EXISTS profiles (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT UNIQUE,
			url TEXT UNIQUE,
			bl_id INTEGER,
			content TEXT
		)""")
		# Create index
		cur.execute("""CREATE INDEX IF NOT EXISTS bl_id ON profiles (bl_id)""")

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
				cur.execute("""UPDATE topics SET title = ?, content = ?, last_update = ? WHERE id = ?""", (topic.title, topic.content, topic.timestamp, topic_id))
			# Add new topic
			else:
				cur.execute("""INSERT OR IGNORE INTO topics(title, author, author_url, content, url, last_update)
					VALUES (?, ?, ?, ?, ?, ?)""", (topic.title, topic.author, topic.author_url, topic.content, topic.url, topic.timestamp))
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
			if not file.topic or not file.topic.id:
				continue

			cur.execute("""INSERT OR IGNORE INTO files(name, url, topic_id)
				VALUES (?, ?, ?)""", ( file.name, file.url, file.topic.id ))

		self._finish()

	# Add a new profile
	def add_profile(self, profiles):
		cur = self._start()

		if isinstance(profiles, ForumProfile):
			profiles = [profiles]

		for profile in profiles:
			if not isinstance(profile, ForumProfile):
				continue

			cur.execute("""INSERT OR IGNORE INTO profiles(name, url, bl_id, content)
				VALUES (?, ?, ?, ?)""", (profile.name, profile.url, profile.bl_id, profile.content))

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

	# Get all files as a generator
	def get_files(self):
		cur = self._start();

		for row in cur.execute("SELECT url FROM files"):
			yield row[0]

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
			"download": None,
			"download_only": False,
			"timeout": 10,
			"retries": 1,
			"threads": get_core_count(),
			"verbose": 0
		})

		AntiDomainBasher.timer = self.settings.sleep_block

	# Go through the links
	def process(self, urls):
		import os.path

		if self.settings.download and not os.path.isdir(self.settings.download):
			os.makedirs(self.settings.download)

		with Database.connect(self.dbfile) as db:

			self.settings.latest_update = db.get_latest_timestamp()

			if self.settings.verbose > 1:
				print("Searching through forum...")

			# Prepare urls
			if not self.settings.download_only:
				items = [ForumBoard(self.settings, url) for url in urls]
			else:
				items = [ArchiveFile(self.settings, None, url) for url in db.get_files()]

			# Decorator
			def load(data):
				return data.load()

			visited = set()

			for item in items:
				visited.add(item.url)

			# Initialize the pool
			with futures.ThreadPoolExecutor(max_workers=self.settings.threads) as e:
				future_data = {e.submit(load, item): item for item in items}

				# Keep going until list is empty
				while len(future_data):
					if self.settings.verbose > 1:
						print("Queued: " + str(len(future_data)))
					for future in futures.as_completed(future_data):
						request = future_data[future]
						del future_data[future]

						try:
							data = future.result()
						except Exception as exc:
							import traceback
							if self.settings.verbose > 0:
								print(traceback.format_exc())
								print("Error: (" + str(type(request)) + ") " + str(exc))
						else:
							# Something bad happened
							if data == False:
								continue
							# Handle types
							if isinstance(request, ForumTopic):
								db.add_topic(request)
							elif isinstance(request, ForumProfile):
								if data == True:
									db.add_profile(request)
								continue
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
		lock = AntiDomainBasher.wait_for_lock(self.url)
		if not lock:
			return False
		with lock:
			if self.settings.verbose > 1:
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
		topics = [ForumTopic(self.settings, a.text, a['href'], remove_by(a.find_parent('td').find_next_siblings(name='td')[-1].span.text)) for a in soup.find_all(href=self.is_topic_link) if a.find_parent('td').name == 'td']
		# Remove old topics
		topics = [topic for topic in topics if topic.timestamp > self.settings.latest_update]
		# Together
		if len(topics):
			boards.extend(topics)
		else:
			boards = []
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
		self.content = ""
		self.url = url or ""
		self.date = date or ""

		self.zips = []

		self.settings = settings

		# Cache
		self._timestamp = None

	# Load the page and get content
	def load(self):
		lock = AntiDomainBasher.wait_for_lock(self.url)
		if not lock:
			return False
		with lock:
			if self.settings.verbose > 1:
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

		files = []

		# Locate author
		def is_profile_link(href):
			return href and re.compile('\?action\=profile;u\=([0-9]*)$').search(href)
		profile = soup.find(href=is_profile_link)
		if profile:
			self.author = profile.text
			self.author_url = profile['href']
			files.append(ForumProfile(self.settings, self.author_url))

		# Locate first post
		post = soup.find(class_="post")
		if post:
			self.content = str(post)
			# Find all links in this post
			files.extend([ArchiveFile(self.settings, self, link['href']) for link in post.find_all(name='a')])

		return files

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

# Handles a user profile
class ForumProfile:
	# Constructor
	def __init__(self, settings, url):
		self.name = ""
		self.url = url
		self.bl_id = -1
		self.content = ""

		self.settings = settings

	# Load the profile
	def load(self):
		lock = AntiDomainBasher.wait_for_lock(self.url)
		if not lock:
			return False
		with lock:
			if self.settings.verbose > 1:
				print("Profile: " + self.url)
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

		# Fix problem with html
		text = re.compile('<\/td>\s*<\/td>').sub('</td>', resp.text)

		# Parse the response
		soup = BeautifulSoup(text, 'html.parser')

		# Decorators
		def is_profile_name(tag):
			return tag and re.compile('^Name:').search(tag.text)
		def is_profile_bl_id(tag):
			return tag and re.compile('^Blockland ID:').search(tag.text)

		# Locate name
		name = soup.find(is_profile_name)
		if name:
			if name.name != 'td':
				name = name.find_parent('td')
			self.name = name.find_next_sibling('td').text

		# Locate BL_ID
		bl_id = soup.find(is_profile_bl_id)
		if bl_id:
			if bl_id.name != 'td':
				bl_id = bl_id.find_parent('td')
			self.bl_id = bl_id.find_next_sibling('td').text

		# Store original content
		if name:
			self.content = str(name.find_parent('table'));
		elif bl_id:
			self.content = str(bl_id.find_parent('table'))
		else:
			self.content = str(soup)

		return True

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
		guess = self.guess_filename()
		# Guess by downloading as well
		if self.settings.download and self.name and guess:
			guess = self.download(self.settings.download)
		return guess

	# Guess the filename it could have
	def guess_filename(self):
		status = None
		url = self.url
		lock = AntiDomainBasher.wait_for_lock(self.url)
		if not lock:
			return False
		with lock:
			if self.settings.verbose > 1:
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
					# Update url for redirects
					url = resp.url
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
					# Update url for redirects
					url = resp.url

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
		file = self.get_url_file(url)
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

	# Download file and save it
	def download(self, path):
		import os.path, os, zipfile
		if not self.name:
			return False
		status = None

		lock = AntiDomainBasher.wait_for_lock(self.url)
		if not lock:
			return False
		with lock:
			if self.settings.verbose > 1:
				print("Download: " + self.url)
			# Try to download the file
			for _ in range(self.settings.retries + 1):
				try:
					resp = requests.get(self.url, timeout=self.settings.timeout, stream=True)
				except requests.exceptions.Timeout:
					pass # Try again
				except requests.exceptions.RequestException:
					return False
				else:
					status = resp.status_code
					break

		if status != 200:
			return False

		file = os.path.join(path, self.name)

		# Save to a file
		with open(file, 'wb') as f:
			for chunk in resp.iter_content(4096):
				f.write(chunk)

		# Check if not zip file and then remove it
		if not zipfile.is_zipfile(file):
			# TODO: If possible, actually try to get the correct link out from this
			os.remove(file)
			return False
		return True

	# Get file from url
	@staticmethod
	def get_url_file(url):
		return url.split('/')[-1].split('?')[0].split('#')[0]

# Main function
# Start here
def main(argv):

	# Default values
	threads = None
	timeout = None
	retries = None
	download = None
	download_only = False
	sleep_block = None
	verbose = 0
	dbfile = 'blforum.sqlite'
	urls = ["https://forum.blockland.us/index.php?board=34.0"]

	# Get arguments
	try:
		opts, args = getopt.getopt(argv[1:], "t:r:j:d:b:v", ["db=", "download-only"])
	except getopt.GetoptError:
		print("Invalid parameters")
		return 2
	else:
		for opt, arg in opts:
			if opt == '-j':
				threads = int(arg)
			elif opt == '-t':
				timeout = int(arg)
			elif opt == '-r':
				retries = int(arg)
			elif opt == '-d':
				download = arg
			elif opt == '-b':
				arg = [int(i) for i in arg.split(',')];
				if len(arg) == 2:
					sleep_block = tuple(arg)
				elif len(arg) == 1:
					sleep_block = arg[0]
				else:
					print("Parameter -b should either be a tuple(2) or a single integer")
					return 2
			elif opt == '-v':
				verbose += 1
			elif opt == '--db':
				dbfile = arg
			elif opt == '--download-only':
				download_only = True
		# Got some urls
		if len(args) > 0:
			urls = args

	# Create main object
	forum = BlocklandForumScraper(db=dbfile)

	# Apply settings
	if threads:
		forum.settings.threads = threads
	if timeout:
		forum.settings.timeout = timeout
	if retries:
		forum.settings.retries = retries
	if download:
		forum.settings.download = download
	if sleep_block:
		forum.settings.sleep_block = sleep_block
	
	if download_only and not download:
		print("Option --download-only require option -d to function properly")
		return 1
	forum.settings.download_only = download_only
	forum.settings.verbose = verbose

	# Process the urls
	forum.process(urls)

	return 0

if __name__ == "__main__":
	sys.exit(main(sys.argv))
