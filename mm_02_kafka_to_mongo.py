import pymongo
from common import common
from kafka import KafkaConsumer
import pymongo
import sys
import datetime

# 실행 파라미터를 주지 않을 경우, 디폴트 값으로 실행 된다.
if sys.argv[1] != None and sys.argv[2] != None:
  target_db = sys.argv[1]
  target_col = sys.argv[2]
else :
  target_col = 'collect'

def kafka_connection(ip, port, topic):
  server_addr = "{0}:{1}".format( ip, port )
  try :
    consumer = KafkaConsumer(topic, bootstrap_servers=[server_addr])
    return consumer 
  except :
    print('kafka_fail')
    return False

def cal_val ( mongoTime, currentTime ) :
        time1 = currentTime
        time2 =  mongoTime
        minus_time = time1 - time2
        cal_time = minus_time.total_seconds()
        return cal_time

def insert_mongo(): # collection list 를 던져 주야 한다
  pass

def get_collection(p_time, p_collection_name ,p_chk_split): # tag, collection_list(db, collection name, yyyymmdd), 
  tmp_day, tmp_time = p_time.split(' ')
  yyyy, mm, dd = tmp_day.split('-')
  if p_chk_split == "yyyy":
    return yyyy
  elif p_chk_split == "mm":
    return "{0}_{1}{2}".format(p_collection_name, yyyy, mm)
  elif p_chk_split == "dd":
    return "{0}_{1}{2}{3}".format(p_collection_name, yyyy, mm, dd)
  else :
    return None



if __name__ == "__main__":
  init = common()
  init.my_database_conn()

  # Kafka 접속 정보
  DB = ""
  COLLECTION = ""
  kafka_infos = init.my_table_select('SP_GET_CONNECT_INFO', 'KafkaCon001', '')
  
  kafka_ip = kafka_infos[1]
  kafka_port = kafka_infos[2]
  kafka_topic = target_col 
  consumer = kafka_connection( kafka_ip, kafka_port, kafka_topic )
  if consumer == False :
    exit()

  # save 접속 정보
  mongo_addrs = init.my_table_select('SP_GET_SAVE_INFO', 'MongoCon001', '')
  mongo_ip = mongo_addrs[1]
  mongo_port = int(mongo_addrs[2])

  print(mongo_ip, mongo_port)

  # 저장 정보 만들기
  init.my_conn('172.17.1.34', 3306, 'root', 'dt01@', 'fems')

  # 오라클 접속 정보
  oracle_conn_info = init.my_select( 'SP_ORACLE_CONN', '')
  oracle_ip = oracle_conn_info[0][0]
  oracle_port = oracle_conn_info[0][1]
  oracle_id = oracle_conn_info[0][2]
  oracle_pw = oracle_conn_info[0][3]

  # 접속 정보 
  sql = init.my_select('SP_GET_SQL', ('sql_07' ,))
  sql = sql[0][0]
  conn_list = init.oracle_table_select( oracle_ip, oracle_port, oracle_id, oracle_pw, sql )

  # 저장을 위해 태그별 저장 위치 생성
  conn_info = {}
  mongo_clinet = {}
  for row in conn_list:
    tag_store_table_id = row[0]
    store_db = row[1]
    storage_table = row[2]
    ip_addr = row[3]
    port = row[4]
    cycle = row[5]
    mongo_adrres_key = ip_addr + ":" + str(port)
    if mongo_adrres_key  not in mongo_clinet:
      MongoDB_Connection = pymongo.MongoClient(ip_addr, port)
      mongo_clinet[mongo_adrres_key] = MongoDB_Connection
    conn_info[tag_store_table_id] = {'db' : MongoDB_Connection[store_db],'table' : storage_table, 'cycle' : cycle}

  # 태그와 저장 정보 
  sql = init.my_select('SP_GET_SQL', ('sql_08' ,))
  sql = sql[0][0]
  tag_list = init.oracle_table_select( oracle_ip, oracle_port, oracle_id, oracle_pw, sql )

  # 태그와 스토리지를 키로 묶음
  tag_save_info = {}
  for row in tag_list:
    tag_save_info[row[0]] = row[1]
    
  # kafka에서 들어온 데이터를 mongodb에 저장한다 
  ###########################################################
  st_time = 180
  late_time  = datetime.datetime.now()
  for message in consumer:
    server_time = str(datetime.datetime.now())
    value = (message.value).decode("utf-8")
    value = value.split("||")
    tag_id = value[0]
 
    # 해당 접속  정보 재수행한다.
    curr_time = datetime.datetime.now()
    update_time = cal_val( late_time, curr_time )
    if st_time < update_time : 
      late_time = curr_time
      # 접속 정보
      sql = init.my_select('SP_GET_SQL', ('sql_07' ,))
      sql = sql[0][0]
      conn_list = init.oracle_table_select( oracle_ip, oracle_port, oracle_id, oracle_pw, sql )

      # 저장을 위해 태그별 저장 위치 생성
      conn_info = {}
      mongo_clinet = {}
      for row in conn_list:
        tag_store_table_id = row[0]
        store_db = row[1]
        storage_table = row[2]
        ip_addr = row[3]
        port = row[4]
        cycle = row[5]
        mongo_adrres_key = ip_addr + ":" + str(port)
        if mongo_adrres_key  not in mongo_clinet:
          MongoDB_Connection = pymongo.MongoClient(ip_addr, port)
          mongo_clinet[mongo_adrres_key] = MongoDB_Connection
        conn_info[tag_store_table_id] = {'db' : MongoDB_Connection[store_db],'table' : storage_table, 'cycle' : cycle}

      # 태그와 저장 정보
      sql = init.my_select('SP_GET_SQL', ('sql_08' ,))
      sql = sql[0][0]
      tag_list = init.oracle_table_select( oracle_ip, oracle_port, oracle_id, oracle_pw, sql )

      # 태그와 스토리지를 키로 묶음
      tag_save_info = {}
      for row in tag_list:
        tag_save_info[row[0]] = row[1]

    #########################################################


    if value[0] == ' ' or  value[0] == '':
      pass
    else:
      # 저장소 정보가 mappint table 에 없으면 pass
      if tag_id in tag_save_info:
        storage_id = int(tag_save_info[tag_id])
        db = conn_info[storage_id]['db']
        table = conn_info[storage_id]['table']
        cycle = conn_info[storage_id]['cycle']
        collection_class = get_collection( value[2], table, cycle )
   
        # 숫자인거 
        try :
          db[collection_class].insert_one({"tag" : value[0], "val" : float(value[1]), "time" : value[2], "kind" : value[3], "factory_id" : value[4], 'server_time' : server_time})
        # 문자 인거
        except :
          db[collection_class].insert_one({"tag" : value[0], "val" : value[1], "time" : value[2], "kind" : value[3], "factory_id" : value[4], 'server_time' : server_time})


#      db[collection_class].insert_one({"tag" : value[0], "val" : value[1], "time" : value[2], "kind" : value[3], "factory_id" : value[4]})










