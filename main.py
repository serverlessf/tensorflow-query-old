from http.server import HTTPServer, BaseHTTPRequestHandler
from http.client import HTTPConnection
from urllib.parse import urlparse
import optparse
import sqlite3
import json
import re



def sql_type(val):
  return type(val)

def addr_parse(addr):
  i = addr.find(':')
  host = '' if i<0 else addr[0:i]
  port = int(addr if i<0 else addr[i+1:])
  return (host, port)

def sql_replace(table, row):
  fields = ', '.join(['"%s"' % k for k in row])
  values = ', '.join([':%s' for k in row])
  return 'REPLACE INTO "%s" (%s) VALUES (%s)' % (table, fields, values)

def db_replace(c, table, row):
  keys = c.execute('SELECT * FROM "%s" LIMIT 1' % table).keys()
  for k in row:
    if k not in keys:
      c.execute('ALTER TABLE "%s" ADD "%s" %s' % (table, k, sql_type(k)))
  c.execute(sql_replace(table, row), row)



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
    return db.execute(query).fetchall()

  def replace(self, name, addr, row):
    db = self.db.cursor()
    row = db.execute('SELECT * FROM "services" WHERE "name"=?', name).fetchone()
    if row is not None and row['addr'] != addr:
      return 'Cant access %s!' % name
    row.update({'name': name, 'addr': addr})
    db_replace(db, 'services', row)

  def remove(self, name, addr):
    db = self.db.cursor()
    row = db.execute('SELECT * FROM "services" WHERE "name"=?', name)
    if row is None or row['addr'] != addr:
      return 'Cant access %s!' % name
    db.execute('DELETE FROM "services" WHERE "name"=?', name)

  def do_GET(self, http):
    req = urlparse(http.path)
    return http.send_json(200, self.get(req.params['query']))
  
  def do_POST(self, http):
    name = http.path[1:]
    addr = http.address_string()
    row = http.body_json()
    err = self.replace(name, addr, row)
    code = 200 if err is None else 400
    return http.send_json(code, {'error': err})

  def do_DELETE(self, http):
    name = http.path[1:]
    addr = http.address_string()
    err = self.remove(name, addr)
    code = 200 if err is None else 400
    return http.send_json(code, {'error': err})

  def start(self, addr):
    db = self.db.cursor()
    name = '"name" TEXT PRIMARY KEY'
    addr = '"addr" TEXT'
    db.execute('CREATE TABLE IF NOT EXISTS "services" (%s, %s)' % (name, addr))
    httpd = HTTPServer(addr, RequestHandler)
    httpd.handler = self
    httpd.serve_forever()


p = optparse.OptionParser()
p.set_defaults(addr='1992', db='main.db')
p.add_option('--addr', dest='addr', help='set net address')
p.add_option('--db', dest='db', help='set database file')
(o, args) = p.parse_args()

addr = addr_parse(o.addr)
serv = ServiceHandler(o.db)
print('Starting service on', addr)
serv.start(addr)
