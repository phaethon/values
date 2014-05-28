import shlex
import hashlib
import time
import readline
import pymongo
import bson
from bson.objectid import ObjectId
from pyparsing import Keyword, Literal, NoMatch, Optional, ParseException, QuotedString, Suppress, White, Word, ZeroOrMore, alphanums, alphas, hexnums, nums, pythonStyleComment, restOfLine, stringEnd, stringStart 

DATABASE = 'test'
class Config(object):
  pass
config = Config()
config.user = 'username'

commands = ['add', 'collections', 'comment', 'delete', 'drop', 'exit', 'find', 'help', 'leave', 'list', 'log', 'quit', 'remove', 'rename', 'search', 'show', 'tag', 'update', 'untag', 'use']

doc = {}

def splitkv(s):
  ind = s.index('=')
  return { 'key': s[:ind], 'value': s[ind+1:]}

def padid(s):
  s += '0' * (24 - len(s))
  return ObjectId(s)

def print_help(cmd):
  d = 'Documentation on %s in progress... :)' % cmd
  docs = {
    'add':
      "Use add to create new record or new collection.\nCOL_NAME add FIELD1=VALUE1 FIELD2=""VALUE2"" ...",
    'collections':
      "List all available collections.\ncollections",
    'remove':
      "Remove record.\nremove OBJECT_ID",
    'leave':
      "Forget current default collection. Used after 'use'",
    'list':
      "List entries in the collection.\nCOL_NAME list",
    'log':
      "Show log of activities.\nlog",
    'show':
      "Show individual record. It is sufficient to enter only first digits of OBJECT_ID as long as it is not ambigous.\nshow OBJECT_ID",
    'unknown':
      "Unknown collection or command. Use 'help' to learn commands. Use 'collections' to list collections.",
    'use':
      "Select current default collection. This collection will be assumed the COL_NAME for other commands until you issue 'leave'\nuse COL_NAME"
  }
  if cmd in docs:
    d = docs[cmd]
  print(d)

def collect_doc(tokens):
  key = tokens['key']
  key = key.lower()
  if 'int' in tokens:
    doc[key] = int(tokens['int'])
  elif 'float' in tokens:
    print(tokens['float'])
    doc[key] = float('.'.join(tokens['float']))
  elif 'string' in tokens:
    doc[key] = tokens['string']
  else:
    raise(Exception("Unknown value type"))

def generate_id(doc):
  return bson.objectid.ObjectId(hashlib.md5(str(doc).encode('utf-8')).hexdigest()[:24])

mongo_client = pymongo.MongoClient()
databases = mongo_client.database_names()
db = mongo_client[DATABASE]

def get_value_collections(self):
  self._collections = self._collections.union(set(self.collection_names()))
  if 'system.indexes' in self._collections:
    self._collections.remove('system.indexes')
  return self._collections

def find_partial(self, oid):
  low = padid(oid)
  high = padid(hex(int(oid, 16) + 1)[2:])
  r = self.find({'_id': {'$gte': low, '$lt': high}}).count()
  if r>1:
    raise(Exception("%s is ambigous and matches %d ObjectId records" % (oid, r)))
  elif r == 1:
    return self.find_one({'_id': {'$gte': low, '$lt': high}})
  else:
    return None

db._collections = set(db.collection_names())
pymongo.database.get_value_collections = get_value_collections
del get_value_collections
pymongo.database.Database.collections = property(pymongo.database.get_value_collections)
pymongo.collection.Collection.find_partial = find_partial
del find_partial

collection = ''

objectid = Word(hexnums)
numeric = (Word(nums).leaveWhitespace() + Suppress(Literal('.')).leaveWhitespace() + ZeroOrMore(Word(nums)))("float") | Word(nums)("int")  
value = QuotedString(quoteChar="\"", escChar = '\\', unquoteResults = True)("string") \
  | QuotedString(quoteChar="'", escChar = '\\', unquoteResults = True)("string")| numeric | Word(alphanums)("string")
field_name = Word(alphas + '_', alphanums + '_')('key')
kvpair = field_name + Suppress('=') + value
kvpair.setParseAction(collect_doc)
col_name = Word(alphas, alphanums + '_')
exit_cmd = Keyword('exit')("cmd") | Keyword('quit')("cmd")
remove_cmd = (Keyword('remove')("cmd") | Keyword('delete')('cmd')) + objectid("objectid") 
help_cmd = Keyword('help')('cmd') + Optional(col_name)("help_cmd")
show_cmd = Keyword('show')('cmd') + objectid("objectid")
use_cmd = Keyword('use')('cmd') + col_name('col')
list_cmd = Keyword('list')('cmd')
col_cmd = Keyword('collections')('cmd')
leave_cmd = Keyword('leave')('cmd')
log_cmd = Keyword('log')('cmd')
add_cmd = Keyword('add')("cmd") + ZeroOrMore(kvpair)
main_cmd = help_cmd | exit_cmd | use_cmd | leave_cmd | add_cmd | list_cmd | col_cmd | log_cmd | show_cmd | remove_cmd
pipe_cmd = NoMatch()
full_cmd = main_cmd + ZeroOrMore( '|' + pipe_cmd)
input_line = (stringStart + full_cmd + stringEnd) | (stringStart + stringEnd)
col_start = (stringStart + col_name("col") + restOfLine("rest")) | (restOfLine("rest"))
input_line.ignore(pythonStyleComment)

try:
  while True:
    new_col = False 

    line = input('%s> ' % collection)
    try:
      parse = col_start.parseString(line)
      if 'col' in parse and (parse['col'] in db.collections):
        line = parse['rest']
        col = parse['col']          
      elif 'col' in parse and parse['col'] in commands:
        col = collection
      elif 'col' in parse:
          col = parse['col']
          new_col = True
          line = parse['rest']
      else:
        col = collection
      doc = {}
      parse = input_line.parseString(line)
#      print(parse.dump())
    except ParseException as e:
      print("Failed to parse input line.")
      print_help('unknown')
      continue
    if new_col and parse['cmd'] != 'add':
      print_help('unknown')
      continue
    if len(parse) == 0 or parse["cmd"] == 'help':
      if 'help_cmd' in parse:
        print_help(parse['help_cmd'])
      else:
        print('Available commands:')
        print(', '.join(commands))
        print('To get started check out list, add, and show commands.')
      continue
    elif parse["cmd"] == 'quit' or parse["cmd"] == 'exit':
      break
    elif parse['cmd'] == 'use':
      if parse['col'] in db.collections:
        collection = parse['col']
        print("Collection %s selected." % collection)
      else:
        print("Collection %s not found. Use 'add' to create new collection." % parse['col'])
        continue
    elif parse['cmd'] == 'leave':
      if collection:
        print("Leaving collection %s." % collection)
        collection = ''
      else:
        print("No currently selected collection.")
    elif parse['cmd'] == 'add':
      if not col:
        print_help('add')
      else:
        if new_col:
          r = input("Collection %s does not exist. Do you want to create it? (y/n)" % col)
          if r.lower() != 'y':
            continue
        doc['_created'] = time.time()
        doc['_updated'] = doc['_created']
        doc['_id'] = bson.objectid.ObjectId(hashlib.md5(str(doc).encode('utf-8')).hexdigest()[:24])
        db[col].insert(doc)
        doc['_action'] = 'add'
        doc['_user'] = config.user
        doc['_doc_id'] = doc['_id']
        doc['_id'] = bson.objectid.ObjectId(hashlib.md5(str(doc).encode('utf-8')).hexdigest()[:24])
        db['log.activities'].insert(doc)
    elif parse['cmd'] == 'collections':
      print(db.collections)
    elif parse['cmd'] == 'list':
      if not col:
        print_help('list')
        continue
      for item in db[col].find():
        print(item) 
    elif parse['cmd'] == 'log':
      if not 'log.activities' in db.collections:
        print("Log is not created. It will be automatically created after add, remove, update or other commands, which change database.")
        continue
      for item in db['log.activities'].find().sort('_updated', -1):
        print(item)
      
    elif parse['cmd'] == 'show':
      try:
        r = db[col].find_partial(parse['objectid']) 
        if r:
          print(r)
          continue
        else:
          print("ObjectId starting with %s is not found." % parse['objectid'])
      except Exception as e:
        print(e)  
    elif parse['cmd'] == 'delete' or parse['cmd'] == 'remove':
      try:
        doc = db[col].find_partial(parse['objectid']) 
        if doc:
          ans = input("Are you sure you want to delete record %s? (y/n)" % doc)
          if ans.lower() == 'y':
            db[col].remove(doc, True)
            doc['_action'] = 'remove'
            doc['_user'] = config.user
            doc['_doc_id'] = doc['_id']
            doc['_id'] = bson.objectid.ObjectId(hashlib.md5(str(doc).encode('utf-8')).hexdigest()[:24])
            db['log.activities'].insert(doc)
        else:
          print("ObjectId starting with %s is not found." % parse['objectid'])
      except Exception as e:
        print(e)  
#    elif cmd == 'drop':
#      if len(tokens) != 2:
#        print("Use 'drop COLLECTION_NAME'")
#        continue
#      else:
#        col = tokens[1]
#      if col not in db.collections:
#        print("Collection %s not found." % col)
#        continue
#      else:
#        ans = input("Are you sure you want to drop collection %s? (y/n)" % col)
#        if ans.lower() == 'y':
#          db[col].drop()
#        continue
#    elif cmd == 'update':
#      if len(tokens) < 3 or len(tokens[1]) > 24:
#        print("Use 'COLLECTION_NAME update OBJECT_ID key=value'")
#        continue
#      try:
#        r = db[col].find_partial(tokens[1]) 
#        if r:
#          doc = { splitkv(pair)['key']: splitkv(pair)['value'] for pair in tokens[2:]}
#          r.update(doc) 
#          r['_updated'] = time.time()
#          db[col].save(r)
#          continue
#        else:
#          print("ObjectId starting with %s is not found." % tokens[1])
#      except Exception as e:
#        print(e)  
#    elif cmd == 'tag':
#      pass
#    elif cmd == 'untag':
#      pass
#    elif cmd == 'find' or cmd == 'search':
#      print("Not implemented yet")
#      continue
#    if piped:
#      pass
except EOFError:
  print("Exiting...")
