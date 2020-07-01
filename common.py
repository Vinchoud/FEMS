import os 
import cx_Oracle
import pymysql
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
class common :
  def __init__(self):
    os.putenv('NLS_LANG', '.UTF8')

  def my_database_conn(self):
    ConInfoFile = open('/home/seah/app/bestFems/connect.conf', 'r', encoding='UTF8')
    Coninfo = ConInfoFile.read()
    ConInfoFile.close()

    ConData = Coninfo.split(',')

    
    self.ip = ConData[0]
    self.port = ConData[1]
    self.user = ConData[2]
    self.pw = ConData[3].rstrip('\n')
    
    self.string = self.user+'/'+self.pw+'@'+self.ip+':'+self.port+'/BESTSF'

  def my_table_select(self, p_sp_name, p_sp_param, p_sp_param2):
    result = []
    try :
      conn = cx_Oracle.connect(self.string)
      cur = conn.cursor()
      if(p_sp_name == 'SP_GET_CONNECT_INFO'):
         ip_cur = cur.var(cx_Oracle.STRING)
         port_cur = cur.var(cx_Oracle.STRING)
         topic_cur = cur.var(cx_Oracle.STRING)
         user_cur = cur.var(cx_Oracle.STRING)
         pw_cur = cur.var(cx_Oracle.STRING)
         query = cur.callproc(p_sp_name, [p_sp_param, ip_cur, port_cur, topic_cur, user_cur, pw_cur])
      elif(p_sp_name == 'SP_GET_SAVE_INFO'):
         ip_cur = cur.var(cx_Oracle.STRING)
         port_cur = cur.var(cx_Oracle.STRING)
         query = cur.callproc(p_sp_name, [p_sp_param, ip_cur, port_cur])
      elif(p_sp_name == 'SP_GET_SUM_INTERVAL'):
         interval = cur.var(cx_Oracle.STRING)
         cycle = cur.var(cx_Oracle.STRING)
         table = cur.var(cx_Oracle.STRING)
         query = cur.callproc(p_sp_name, [p_sp_param, p_sp_param2, interval, cycle, table])
       
      result = query

      cur.close()
      conn.close()
    except : 
      result =  "error : "  

    
    return result 

  # 오라클 sp 실행
  def oralce_sp_exec(self, p_ip, p_port, p_id, p_pw, p_sp, p_param = []):
    oracle_conncetion_format = "{0}/{1}@{2}:{3}/bestsf".format( p_id, p_pw, p_ip, p_port)
    conn = cx_Oracle.connect(oracle_conncetion_format)
    db = conn.cursor()
    db.callproc(p_sp, p_param)
    conn.commit()
    db.close()
    conn.close()

  # 오라클 테이블 조회
  def oracle_table_select(self, p_ip, p_port, p_id, p_pw, p_sql):
    oracle_conncetion_format = "{0}/{1}@{2}:{3}/bestsf".format( p_id, p_pw, p_ip, p_port)
    conn = cx_Oracle.connect(oracle_conncetion_format)
    db = conn.cursor()
    db.execute(p_sql)
    tag_list = []
    for recode in db:
      tag_list.append( recode )
    db.close()
    conn.close()
    return tag_list

  # 오라클 insert 
  def oracle_bulk_insert(  self, p_ip, p_port, p_id, p_pw, p_sql, p_insert_list ):
    oracle_conncetion_format = "{0}/{1}@{2}:{3}/bestsf".format( p_id, p_pw, p_ip, p_port)
    conn = cx_Oracle.connect(oracle_conncetion_format)
    db = conn.cursor()
    db.executemany(p_sql, p_insert_list)
    conn.commit()
    db.close()
    conn.close()

  # mysql 연결
  def my_conn(self, p_ip, p_port, p_id, p_pw, p_db = 'fems'):
    self.ip = p_ip
    self.port = p_port
    self.user = p_id
    self.pw = p_pw
    self.MariaDataBaseName = p_db

  # mysql 조회
  def my_select(self, p_sp_name, p_sp_param):
    result = []
    try :
      conn = pymysql.connect(host=self.ip, port=self.port, user=self.user, password=self.pw, db=self.MariaDataBaseName, charset='utf8')
      cur = conn.cursor()
      cur.callproc(p_sp_name, p_sp_param)
      for row in cur.fetchall():
        result.append( row )
      cur.close()
      conn.close()

    except Exception as e :
      result =  "error : " +  str(e)
      print( result )

    return result

  
  def my_bulk_insert(self, p_sql, p_insert_list):
     conn = pymysql.connect(host=self.ip, port=self.port, user=self.user, password=self.pw, db=self.MariaDataBaseName, charset='utf8')
     cur = conn.cursor()
     cur.executemany(p_sql, p_insert_list)
     conn.commit()
     cur.close()
     conn.close()


  # 특정 규칙으로 해당 table를 리텅해 준다.
  def create_collection(self, p_time, p_collection_name ,p_chk_split): # tag, collection_list(db, collection name, yyyymmdd),
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
  
  # from ~ to 까지의 시간을 주면 해당 규칙으로 조회 table 리스트를 던져준다
  def get_collection(self, from_time, to_time, p_collection_name ,p_chk_split): # tag, collection_list(db, collection name, yyyymmdd),
    init = common()
    collection_list = []
    from_time = init.str_to_time( from_time )
    to_time = init.str_to_time( to_time )
    # 월별 생성 시
    if p_chk_split == 'mm':
      # 시간 보정
      from_time = from_time.strftime('%Y-%m-01 00:00:00')
      to_time = to_time.strftime('%Y-%m-01 00:00:00')

      from_time = init.str_to_time( from_time )
      to_time = init.str_to_time( to_time )
      # 해당 값 추적
      temp_number = 0
      temp_number_list = []
      while True :
        chk_time = from_time + relativedelta( months = temp_number )
        if chk_time >= to_time:
          tmp_collection = self.create_collection( chk_time.strftime('%Y-%m-%d %H:%M:00'), p_collection_name, p_chk_split )
          temp_number_list.append( tmp_collection )
          break
        else :
          tmp_collection = self.create_collection( chk_time.strftime('%Y-%m-%d %H:%M:00'), p_collection_name, p_chk_split )
          temp_number_list.append( tmp_collection )
        temp_number = temp_number + 1
    return temp_number_list
  


  def sum_table_update(self, table, tid):
    try:
      conn = cx_Oracle.connect(self.string)
      cur = conn.cursor()
      sql = """UPDATE TAG_SUM_STORE_TABLE SET LAST_CREATE_SUM_TABLE_NAME = '{0}' WHERE TAG_STORE_TABLE_ID = '{1}'""".format(table, tid)
      
      try:
         cur.execute(sql)
         conn.commit()
      except:
         print('Failed to execute cursor')
         printException(exception)
         exit()
      cur.close()
      conn.close()
    
    except:
      printExceptrion(exceptrion)
      exit() 
 
  def time_to_str(self, p_time):
    try :
      change_format = p_time.strftime('%Y-%m-%d %H:%M:%S.%f')
    except :
      change_format = p_time.strftime('%Y-%m-%d %H:%M:%S')
    return change_format


  def str_to_time(self, p_str_time):
    try :
      change_format = datetime.strptime(p_str_time, '%Y-%m-%d %H:%M:%S.%f')
    except : 
      change_format = datetime.strptime(p_str_time, '%Y-%m-%d %H:%M:%S')
    return change_format


  def str_to_time2(self, p_str_time, p_format = '%Y-%m-%d %H:%M:%S'):
    change_format = datetime.strptime(p_str_time, p_format)
    return change_format


  def time_to_str2(self, p_time, p_format = '%Y-%m-%d %H:%M:%S'):
    try :
      change_format = p_time.strftime(p_time, p_format)
    except :
      return False
    return change_format


  def time_minus_time(self, p_from_time, p_to_time, p_seconds = 1):
    temp_minus_time = p_to_time - p_from_time 
    w_time = temp_minus_time.total_seconds() * p_seconds
    return w_time
