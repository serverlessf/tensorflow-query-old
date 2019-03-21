#!/usr/bin/env python3
from http.server import HTTPServer, BaseHTTPRequestHandler
from http.client import HTTPConnection
from urllib.parse import urlparse, parse_qs
import optparse
import sqlite3
import json
import re
import os



SQL_TYPE = {
  'bool': 'REAL',
  'int': 'REAL',
  'float': 'REAL',
  'str': 'TEXT'
}



def sql_type(val):
  mid = str(type(val))[8:-2]
  return SQL_TYPE[mid]

def addr_loads(addr):
  i = addr.find(':')
  host = '' if i<0 else addr[0:i]
  port = int(addr if i<0 else addr[i+1:])
  return (host, port)

def addr_dumps(addr):
  return '%s:%s' % addr

def sql_replace(table, row):
  fields = ', '.join(['"%s"' % k for k in row])
  values = ', '.join([':%s' % k for k in row])
  return 'REPLACE INTO "%s" (%s) VALUES (%s)' % (table, fields, values)

def row_dict(row):
  d = {}
  for k in row.keys():
    d[k] = row[k]
  return d

def db_replace(c, table, row):
  c.execute(sql_replace(table, row), row)

def db_replaceany(c, table, row):
  pragma = c.execute('PRAGMA table_info("%s")' % table).fetchall()
  keys = [col[1] for col in pragma]
  for k in row:
    if k not in keys:
      c.execute('ALTER TABLE "%s" ADD "%s" %s' % (table, k, sql_type(row[k])))
  db_replace(c, table, row)



class RequestHandler(BaseHTTPRequestHandler):
  def body(self):
    size = int(self.headers.get('Content-Length'))
    return self.rfile.read(size)
  
  def body_json(self):
    return json.loads(self.body())

  def send(self, code, body=None, headers=None):
    self.send_response(code)
    for k, v in headers.items():
      self.send_header(k, v)
    self.end_headers()
    if body is not None:
      self.wfile.write(body)
  
  def send_json(self, code, body):
    heads = {'Content-Type': 'application/json'}
    self.send(code, bytes(json.dumps(body), 'utf8'), heads)

  def do_GET(self):
    handler = self.server.handler
    return handler.do_GET(self)

  def do_POST(self):
    handler = self.server.handler
    return handler.do_POST(self)

  def do_DELETE(self):
    handler = self.server.handler
    return handler.do_DELETE(self)



class ServiceHandler:
  db = None

  def __init__(self, db='main.db'):
    self.db = sqlite3.connect(db)

  def get(self, query):
    db = self.db.cursor()
    print(query)
    return db.execute(query).fetchall()

  def replace(self, name, addr, data):
    db = self.db.cursor()
    # row = db.execute('SELECT * FROM "services" WHERE "name"=?', [name]).fetchone()
    # if row is not None and row['addr'] != addr:
    #   return 'Cant access %s!' % name
    data.update({'name': name, 'addr': addr})
    db_replaceany(db, 'services', data)

  def remove(self, name, addr):
    db = self.db.cursor()
    # row = db.execute('SELECT * FROM "services" WHERE "name"=?', [name])
    # if row is None or row['addr'] != addr:
    #   return 'Cant access %s!' % name
    db.execute('DELETE FROM "services" WHERE "name"=?', [name])

  def do_GET(self, http):
    qs = parse_qs(urlparse(http.path).query)
    query = qs['query'][0] if 'query' in qs else 'SELECT * FROM "services"'
    rows = [row_dict(row) for row in self.get(query)]
    return http.send_json(200, rows)
  
  def do_POST(self, http):
    name = http.path[1:]
    addr = addr_dumps(http.client_address)
    row = http.body_json()
    self.replace(name, addr, row)
    err = None
    code = 200 if err is None else 400
    return http.send_json(code, {'error': err})

  def do_DELETE(self, http):
    name = http.path[1:]
    addr = addr_dumps(http.client_address)
    self.remove(name, addr)
    err = None
    code = 200 if err is None else 400
    return http.send_json(code, {'error': err})

  def start(self, addr):
    db = self.db.cursor()
    flds = ('"name" TEXT PRIMARY KEY', '"addr" TEXT')
    db.execute('CREATE TABLE IF NOT EXISTS "services" (%s, %s)' % flds)
    self.db.row_factory = sqlite3.Row
    httpd = HTTPServer(addr, RequestHandler)
    httpd.handler = self
    httpd.serve_forever()


p = optparse.OptionParser()
p.set_defaults(addr=os.environ['PORT'], db='main.db')
p.add_option('--addr', dest='addr', help='set net address')
p.add_option('--db', dest='db', help='set database file')
(o, args) = p.parse_args()

addr = addr_loads(o.addr)
serv = ServiceHandler(o.db)
print('Starting service on', addr)
serv.start(addr)
