import opcua
import time 
import pymongo
import datetime
from kafka import KafkaProducer
import time
import redis

class realtime:

  #redis connection information
  redis_cli0 = redis.Redis(host='localhost', port=6379, db=0)
  redis_cli1 = redis.Redis(host='localhost', port=6379, db=1)

  conn = pymongo.MongoClient('localhost', 5050)
  DB = conn.BigDataCollecter
  Cache = DB.Cache
  jit_list = {}
  tag_list = {}

  def __init__(self, p_kafka_ip, p_kafka_port):
    kafka_connection_info =  "{0}:{1}".format( str(p_kafka_ip), str(p_kafka_port) )
    self.producer = KafkaProducer( bootstrap_servers = kafka_connection_info )

  def sendtokafka(self, kafka_topic, value):
    #print( value )
    for row in value:
       #print(kafka_topic, " : ", row.encode())
       try:
         self.producer.send( kafka_topic, row.encode())
       except:
         print('error')
       #_tag, _val, _time, _kind, _fID = row.split("||")
       #before_val = self.redis_cli1.get(_tag)
       #if before_val == None:
       #   self.redis_cli1.set(_tag, row)
       #else:
       #   self.redis_cli0.set(_tag, before_val)
       #   self.redis_cli1.set(_tag, row)
    #time.sleep(0.5)

  class subHandler(object):
    def __init__(self, p_cache, p_zone, p_producer) : 
      self.cache = p_cache
      self.zone = p_zone
      self.producer = p_producer

      self.producer.send( self.zone, send_format.encode() )

    def datachange_notification(self, node, val, data):
      str(node.nodeid.Identifier).split(".")
      send_format = "{0}||{1}||{2}".format( str(node.nodeid.Identifier), val, datetime.datetime.now(), self.zone )
#      print( send_format )
      self.producer.send( self.zone, send_format.encode() )
#      self.cache.insert_one( {'tag_id' :  str(node.nodeid.Identifier), 'val' : val, "time" : datetime.datetime.now(), "zone" : self.zone } )
#      print("Python: New data change event", node, val)

    def event_notification(self, event):
      pass
#      print("Python: New event", event)

  def opc_connection(self, p_ip, p_port):
    self.opc_connection_format = "opc.tcp://{0}:{1}".format(p_ip, p_port)
    self.client = opcua.Client( self.opc_connection_format )
    self.client.connect()

  def chk_tag(self, p_tag_id, p_tag_name):
    try :
      tag_format = "{0};s={1}".format("ns=2", p_tag_name)
      self.tag = self.client.get_node(tag_format)
#      print( self.tag.get_value() )
      self.tag_list = { p_tag_id : self.tag }
    except : 
      return False
#    print( self.tag_list )
    return p_tag_id 
 
  def del_tag(self, p_tag_id):
    # 수집 가능 여부를 확인
    if p_tag_id in self.tag_list.keys():
      # 수집 종료
      sub = self.jit_list[p_tag_id][0]
      handle = self.jit_list[p_tag_id][1]
      sub.unsubscribe(handle)
      sub.delete()

      # 수집 삭제
      del self.tag_list[p_tag_id]
      del self.jit_list[p_tag_id]
#     print( "tag 수집 종료")



  def realtime_opc_data(self, p_tag_id, p_collection_cycle, p_zone):
    handler = self.subHandler( self.Cache, p_zone, self.producer )
    sub = self.client.create_subscription( p_collection_cycle, handler )
    handle = sub.subscribe_data_change( self.tag_list[p_tag_id] )
    self.jit_list[p_tag_id] = [sub, handle]
'''
tag = 'MASTER_PLC.MASTER_PLC.[DB32_DBW566][A22_PT_003]유압실 펌프3 압력_PIW 2706'
tag_id = 'besteel_bestGunsan_001'
print( "real time" )
realtime = realtime()

realtime.opc_connection("172.31.5.233", "49320")
tags = { tag_id :  tag}
# 테그 수집을 신청하고
result = realtime.chk_tag(tag_id, tag)
print( result )
# 테그 수집을 수행한다. 
realtime.realtime_opc_data(tag_id, 500, 'zone01')

#time.sleep(10)
#realtime.del_tag( 'REAL300' )

#realtime.opc_connection("172.31.5.233", "49320")
#result = realtime.chk_tag(tag_id, tag)
#print( result )
#if result != False :
#  realtime.realtime_opc_data(tag_id, 500)
'''
