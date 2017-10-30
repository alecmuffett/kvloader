#!/usr/bin/env python
# -*- coding: utf-8 -*-
# messing around with key/value tables in python/sql
# copyright 2017 alec.muffett@gmail.com

import bz2
import csv
import gzip
import re
import sqlite3
import sys

# performance
DB = 'tmp.db'
PAGESIZE = 1024 * 64 # number of records-per-import

# initialise database references
connection = None
cursor = None

# TOKEN = [-\.\w]+
# SEPARATOR = ?:[:;]|\s+
RE_PARSE = re.compile(r'^\s*([-\.\w]+)(?:\+\w+)?@([-\.\w]+)(?:[:;]|\s+)([-\.\w!]*)$', re.I)
RE_PARSE_UTF8 = re.compile(r'^\s*([-\.\w]+)(?:\+\w+)?@([-\.\w]+):([-\.\w!]*)$', re.U|re.I)
RE_PARSE_COLONX = re.compile(r'^\s*([-\.\w]+)(?:\+\w+)?@([-\.\w]+)::+([-\.\w!]*)$', re.I)
RE_PARSE_PMAIL = re.compile(r'^\s*(?:\+|%2b)?(\d+)@([-\.\w]+)(?:[:;]|\s+)([-\.\w!]*)$', re.I)
RE_PARSE_LOOSE = re.compile(r'([0-9a-z][-\.\w]*)(?:\+\w+)?@([0-9a-z][-\.\w]*):([-\.\w!]+)', re.I)
RE_PARSE_LOOSE2 = re.compile(r'([0-9a-z][-\.\w]*)(?:\+\w+)?@([0-9a-z][-\.\w]*):([-\.\w\'!"#$%&()*+,/;<=>?@^`|~]{1,12})', re.I)
RE_EMAIL = re.compile(r'^([-\.\w]+@[-\.\w]+)$')
RE_PMAIL = re.compile(r'^(?:\+|%2b)?(\d+@[-\.\w]+)$')
RE_HEX = re.compile(r'^[0-9a-f]+$', re.IGNORECASE)
RE_DELETE = re.compile(r'{newline}', re.IGNORECASE)

# sql
BOOTSTRAP = """
PRAGMA journal_mode = wal;
PRAGMA foreign_keys = ON;
PRAGMA encoding = "UTF-8";
BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS srcs (
    id INTEGER PRIMARY KEY NOT NULL,
    src TEXT NOT NULL UNIQUE
    );
CREATE TABLE IF NOT EXISTS mappings (
    id INTEGER PRIMARY KEY NOT NULL,
    src_id INTEGER NOT NULL,
    key TEXT NOT NULL,
    val TEXT NOT NULL,
    FOREIGN KEY(src_id) REFERENCES srcs(id) ON DELETE CASCADE
    );
PRAGMA user_version = 1;
COMMIT;
"""

CREATE_IMPORTS = """
CREATE TEMPORARY TABLE imports (
  id INTEGER PRIMARY KEY NOT NULL,
  src_id INTEGER NOT NULL,
  key TEXT NOT NULL,
  val TEXT NOT NULL
  );
"""

TRANSFORM_IMPORTS = """
INSERT INTO mappings(src_id, key, val)
SELECT src_id, key, val FROM imports;
"""

CREATE_LOOKUP = """
CREATE TEMPORARY TABLE IF NOT EXISTS lookup (
    id INTEGER PRIMARY KEY NOT NULL,
    word TEXT NOT NULL UNIQUE
    );
"""

# ---- BEGIN LIBRARIES ----

# book-in a source, try to be nice about it
def get_src_id(f):
    s = "SELECT id FROM srcs WHERE src=? LIMIT 1;"
    # search if the source is already known
    cursor.execute(s, (f,))
    x = cursor.fetchall()
    # if not known, insert it & re-search
    if not x:
	# this seems clunky but less prone to error
	i = "INSERT OR IGNORE INTO srcs(src) VALUES(?);"
	cursor.execute(i, (f,))
	cursor.execute(s, (f,)) # source of truth
	x = cursor.fetchall()
    # return our findings
    return x[0][0]

# import buffering
def buffer_init():
    cursor.execute("DROP TABLE IF EXISTS imports;")
    cursor.execute(CREATE_IMPORTS)

def buffer_add(sid, k, v):
    # insert record
    i = "INSERT INTO imports(src_id, key, val) VALUES(?,?,?);"
    cursor.execute(i, (sid, k, v))
    # if necessary, call flush
    n = cursor.lastrowid
    if n and n >= PAGESIZE:
	buffer_flush()

def buffer_flush():
    # if data exists, transform data, no ifs/ands/buts
    cursor.execute("SELECT COUNT(*) FROM imports;")
    x = cursor.fetchall()
    if x and x[0][0] > 0:
	# there are definitely rows
	cursor.execute(TRANSFORM_IMPORTS)
	# re-init
	buffer_init()

# parse a line of input into key and value
def parse(line):
    # try the most common pattern
    mo = RE_PARSE.match(line)
    if mo: # slight wastage here, bwtf consistency
	k = mo.group(1).strip().lower()
	k = '%s@%s' % (k, mo.group(2).strip().lower())
	v = mo.group(3)
	return k, v

    mo = RE_PARSE_PMAIL.match(line)
    if mo:
	k = mo.group(1).strip().lower()
	k = '%s@%s' % (k, mo.group(2).strip().lower())
	v = mo.group(3)
	return k, v

    mo = RE_PARSE_UTF8.match(line)
    if mo:
	k = mo.group(1).strip().lower()
	k = '%s@%s' % (k, mo.group(2).strip().lower())
	v = mo.group(3)
	return k, v

    mo = RE_PARSE_COLONX.match(line)
    if mo:
	k = mo.group(1).strip().lower()
	k = '%s@%s' % (k, mo.group(2).strip().lower())
	v = mo.group(3)
	return k, v

    # search
    mo = RE_PARSE_LOOSE.search(line)
    if mo:
	k = mo.group(1).strip().lower()
	k = '%s@%s' % (k, mo.group(2).strip().lower())
	v = mo.group(3)
	return k, v

    # cleanup
    line = RE_DELETE.sub('', line)
    mo = RE_PARSE_LOOSE2.search(line)
    if mo:
	k = mo.group(1).strip().lower()
	k = '%s@%s' % (k, mo.group(2).strip().lower())
	v = mo.group(3)
	return k, v

    # let's get esoteric
    on_colons = line.split(':')
    if len(on_colons) >= 2:
	mo = RE_EMAIL.match(on_colons[1])
	if not mo:
	    mo = RE_PMAIL.match(on_colons[1])
	if mo:
	    # and there are 2 fields, or a 3rd which is a hexdigest
	    if (len(on_colons) == 2) or \
	       (len(on_colons) == 3 and \
		len(on_colons[2]) == 16 and \
		RE_HEX.match(on_colons[2])):
		k = mo.group(1).strip().lower()
		v = on_colons[0]
		return k, v.strip()

    # give up
    return None, None

# load input from a file object
def load_input(sid, input):
    line_no = 0
    for line in input:
	line_no += 1

	# sanitise a bit
	max_len = 256
	if len(line) > max_len:
	    line = line[0:max_len] # truncate stupidly long input
	line = line.rstrip('\r\n')

        # to unicode, and parse if possible
        u_line = unicode(line, 'utf-8', 'ignore')
	k, v = parse(u_line)
	if k:
	    if v: # skip blank V
		buffer_add(sid, k, v)
	else:
	    print 'reject:', line_no, line # <- not the unicode version

# load a file, switching on suffix
def load_file(f):
    print 'file:', f
    buffer_init()
    sid = get_src_id(f)
    if f.endswith('.bz2'):
	with bz2.BZ2File(f, 'rb') as input:
	    load_input(sid, input) # calls buffer_add
    elif f.endswith('.gz'):
	with GzipFile(f, 'rb') as input:
	    gzip.load_input(sid, input) # calls buffer_add
    else:
	with open(f, 'r') as input:
	    load_input(sid, input) # calls buffer_add
    buffer_flush()

# indexing
def index_name(what):
    return 'fast%sindex' % what

def create_index(what):
    sql = 'CREATE INDEX IF NOT EXISTS %s ON mappings(%s);' % (index_name(what), what)
    cursor.execute(sql)

def drop_index(what):
    sql = 'DROP INDEX %s;' % index_name(what)
    cursor.execute(sql)

# lookups
def lookup_init():
    cursor.execute("DROP TABLE IF EXISTS lookup;")
    cursor.execute(CREATE_LOOKUP)

def lookup_add(word):
    sql = "INSERT INTO lookup(word) VALUES(?);"
    cursor.execute(sql, (word,)) # force tuple with trailing comma

def lookup_run(what, with_source=False):
    if with_source:
	cols = ['key', 'val', 'src_id']
    else:
	cols = ['key', 'val']

    sql = """
    SELECT DISTINCT %s
    FROM mappings m
    WHERE m.%s IN (
	SELECT word FROM lookup
    );
    """ % (
	','.join(cols),
	what
    )
    w = csv.writer(sys.stdout)
    for row in cursor.execute(sql):
	w.writerow(row)

# ---- END LIBRARIES ----

testvec = (
    u'',
)

# user-facing functions

def do_nyi(cargs):
    print 'not yet implemented'

def do_test(cargs):
    for t in testvec:
	print t, '', parse(t)

def do_load(cargs):
    for filename in cargs:
	load_file(filename)

def do_index_key(cargs):
    create_index('key')

def do_index_val(cargs):
    create_index('val')

def do_index_all(cargs):
    do_index_key(cargs)
    do_index_val(cargs)

def do_idrop_key(cargs):
    drop_index('key')

def do_idrop_val(cargs):
    drop_index('val')

def do_idrop_all(cargs):
    do_idrop_key(cargs)
    do_idrop_val(cargs)

def do_key(cargs):
    lookup_init()
    for word in cargs:
	lookup_add(word.strip().lower())
    lookup_run('key')

def do_val(cargs):
    lookup_init()
    for word in cargs:
	lookup_add(word)
    lookup_run('val')

def do_key_file(cargs):
    lookup_init()
    for f in cargs:
	with open(f, 'r') as input:
	    for word in input:
		lookup_add(word.strip().lower())
    lookup_run('key')

def do_val_file(cargs):
    lookup_init()
    for f in cargs:
	with open(f, 'r') as input:
	    for word in input:
		lookup_add(word.strip())
    lookup_run('val')

def do_sources(cargs):
    sql = 'SELECT id, src FROM srcs;'
    w = csv.writer(sys.stdout)
    for row in cursor.execute(sql):
	w.writerow(row)

def do_dump(cargs):
    lookup_init()
    for word in cargs:
	lookup_add(word)
    cols = ['key', 'val']
    sql = """
    SELECT %s
    FROM mappings m
    WHERE m.src_id IN (
	SELECT id
	FROM srcs
	WHERE src IN (SELECT word FROM lookup)
    );
    """ % ','.join(cols)

    w = csv.writer(sys.stdout)
    for row in cursor.execute(sql):
	w.writerow(row)

def do_purge(cargs):
    lookup_init()
    for word in cargs:
	lookup_add(word)
    sql = 'DELETE FROM srcs WHERE src IN (SELECT word FROM lookup)'
    cursor.execute(sql) # let fk do the work

options = {
    'sources': ('', 'print list of previously-loaded sources', do_sources),
    'dump': ('[source ...]', 'extract records from given source(s)', do_dump),
    'purge': ('[source ...]', 'purge records from given source(s)', do_purge),

    'key': ('[key ...]', 'lookup records for key(s)', do_key),
    'key-file': ('[file ...]', 'lookup records for keys cited in file', do_key_file),
    'val': ('[val ...]', 'lookup records for value(s)', do_val),
    'val-file': ('[file ...]', 'lookup records for vals cited in file', do_val_file),
    'test': ('', 'run test vector', do_test),
    'load': ('[file ...]', 'load sources', do_load),
    'index-key': ('', 'index the keys (SLOW, EATS DISK SPACE)', do_index_key),
    'index-val': ('', 'index the vals (SLOW, EATS DISK SPACE)', do_index_val),
    'index-all': ('', 'index everything (SLOW, EATS DISK SPACE)', do_index_all),
    'idrop-key': ('', 'drop the key index', do_idrop_key),
    'idrop-val': ('', 'drop the val index', do_idrop_val),
    'idrop-all': ('', 'drop all indexes', do_idrop_all),
}

def usage(what = None):
    print 'Usage: kvloader [--option [arg ...]]'
    if what: print 'WHAT =', what
    print 'args:'
    for option in sorted(options.keys()):
	print '\t--%-20s %s' % (
	    '%s %s' % (
		option,
		options[option][0]
	    ),
	    options[option][1]
	)

# init
connection = sqlite3.connect(DB)
connection.text_factory = lambda x: unicode(x, 'utf-8', 'ignore') # ignore bad unicode shit
cursor = connection.cursor()
cursor.executescript(BOOTSTRAP)

# die if no args
if len(sys.argv) < 2:
    usage(1)
    sys.exit(1)

# parse out --cmd
cmd = sys.argv[1]
if not cmd.startswith('--'):
    usage(2)
    sys.exit(1)

# check if `cmd` is legit
cmd = cmd[2:]
if not options.get(cmd):
    usage(3)
    sys.exit(1)

# does `cmd` take arguments?
if options[cmd][0] != '':
    cargs = sys.argv[2:] # possibly empty list
else:
    cargs = None # definitely empty list

# call it
fn = options[cmd][2] # get function pointer
fn(cargs)

# finish
connection.commit()
connection.close()

# done
sys.exit(0)
