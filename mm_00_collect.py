#!/usr/local/env python3
# -*-coding:utf-8-*-

import sys
import gc
import socket
import Global
import osLogger
import osCRC
import struct
import datetime
import cx_Oracle
import numpy as np
import threading
import os
import pymongo
import re
import math

from common import common
from multiprocessing import Process
from realtime import realtime
from kafka import KafkaProducer
from getopt import getopt
from getopt import GetoptError
from time import sleep
import uuid

MAX_BUF = 1024 
SIGN_DIGIT = 6
Maker = sys.argv[2]

def Help():
    """
    This function is 'Help Screen'
    """
    try:
        print('Read the register of modbus device')
        print(sys.argv[0], ' -d <Device Config>')
        print('Option Detail')
        print('\t-d --device-conf : Device Configuration Maker, Ex)JB, EL, LS, VI, MI')
        print('\t-s --sync : Thread sync (with join)')
        print('\t-h --help : this screen')
    except Exception as Ex:
        log = osLogger.cOSLogger(_pPrefix='Exception', _pLevel='ERROR')
        log.writeLog(str(Ex))

def readArguments():
    """
    This function parse the arguments
    """
    try:
        opts, args = getopt(sys.argv[1:], 'd:sh', ['device-conf=', 'sync', 'help'])

        DEVICE_FILE = None
        SYNC_FLAG = False

        for opt, arg in opts:
            if (opt == '-d') or (opt == '--device-conf'):
                DEVICE_FILE = arg

            if (opt == '-h') or (opt == '--help'):
                Help()
            
            if (opt == '-s') or (opt == '--sync'):
                print(SYNC_FLAG)
                SYNC_FLAG = True

        if DEVICE_FILE is None:
            log = osLogger.cOSLogger()
            log.printLog('Please, input the device config file option', 'ERROR')
            Help()
            return None

        return SYNC_FLAG

    except GetoptError as Ex:
        log = osLogger.cOSLogger(_pPrefix='Exception', _pLevel='ERROR')
        log.writeLog(str(Ex))
        Help()
        return None

def readDeviceConf(maker : str = None):
    """
    This function parse the arguments
    """
    try:
        if maker is None:
            raise Exception('Invalid Input Maker : None type')

        DeviceConf = dict()
        Device_type = ''
 
        ConInfoFile = open('/home/seah/app/bestFems/connect.conf', 'r', encoding='UTF8')
        Coninfo = ConInfoFile.read()
        ConInfoFile.close
 
        Conin = Coninfo.split(',')
        # Oracle info mapping
        ora_ip = Conin[0]
        ora_port = Conin[1]
        ora_user = Conin[2]
        ora_pw = Conin[3].rstrip('\n')
        ora_string = ora_user+'/'+ora_pw+'@'+ora_ip+':'+ora_port+'/BESTSF'
        ora_con = cx_Oracle.connect(ora_string)
        ora_db = ora_con.cursor()

        if maker == 'JB' or maker == 'EL':
            Device_type = 'C'
            kind = '2'
        elif maker == 'MI' or maker == 'LS' or maker == 'VI' or maker == 'ECO':
            Device_type = 'E'
            kind = '1'

        ora_sql = """SELECT DISTINCT(COM_CONNECT_DEVICE_ID) AS DEVICE_ID FROM DEVICE_MASTER WHERE DEVICE_TYPE = '{0}' AND USE_YN = 'Y' AND MAKER LIKE '%{1}%' """.format(Device_type, maker)
        tfv = ora_db.execute(ora_sql)
        DeviceList = []
        for token in ora_db:
           tmp_row = []
           DeviceID = token[0]
           tmp_row.append(DeviceID)
           DeviceList.append(tmp_row)

        for var in DeviceList:
            ora_sql1 = """SELECT DEVICE_ADDRESS FROM DEVICE_MASTER WHERE DEVICE_TYPE = 'D' AND USE_YN = 'Y' AND DEVICE_ID = '{0}' """.format(var[0])
            fv = ora_db.execute(ora_sql1)
            for token in ora_db:
               Device_addr = token[0]
            DeviceConf[var[0]] = [Device_addr, '1470', 'B', maker, kind]
        
        ora_db.close() 
        ora_con.close()

        #DeviceConf[DeviceList]
        #DeviceConf[DEVICE_INFO[0]] = [DEVICE_INFO[1], DEVICE_INFO[2], DEVICE_INFO[3], DEVICE_INFO[4], DEVICE_INFO[5]]
        return DeviceConf
    except Exception as Ex:
        log = osLogger.cOSLogger(_pPrefix='Exception', _pLevel='ERROR')
        log.writeLog(str(Ex))
        return None

def readDeviceInfo(Device: str = None, maker: str = None):
    try:
        if Device is None:
            raise Exception('Invalid Input Device : None type')

        REGISTERS_INFOS = list()
        DeviceList = list()
        tagList = dict()
        DeviceDic = dict()

        ConInfoFile = open('/home/seah/app/bestFems/connect.conf', 'r', encoding='UTF8')
        Coninfo = ConInfoFile.read()
        ConInfoFile.close
        Conin = Coninfo.split(',')
        # Oracle info mapping
        ora_ip = Conin[0]
        ora_port = Conin[1]
        ora_user = Conin[2]
        ora_pw = Conin[3].rstrip('\n')
        ora_string = ora_user+'/'+ora_pw+'@'+ora_ip+':'+ora_port+'/BESTSF'
        ora_con = cx_Oracle.connect(ora_string)
        ora_db = ora_con.cursor()

        if maker == 'JB' or maker == 'EL':

            ora_sql = """ SELECT DEVICE_ID, DEVICE_ADDRESS, DEVICE_USE FROM DEVICE_MASTER WHERE DEVICE_TYPE = 'C' AND USE_YN = 'Y' AND COM_CONNECT_DEVICE_ID = '{0}' AND MAKER LIKE '%{1}%'""".format(Device, maker)
            tfv = ora_db.execute(ora_sql)
            for token in ora_db:
               tmp_row=[]
               subId = token[0]             
               Deviceaddr = token[1]
               kind = token[2]
               DeviceList.append(subId)       
               DeviceDic[subId] = Deviceaddr
            for var in DeviceList:
                ora_sql1 = """SELECT a.TAG_ID, 
                                     a.DATA_ADDRESS, 
                                     b.FACTORY_ID, 
                                     a.CORRECT_RATIO 
                              FROM TAG_MASTER a
                              inner join DEVICE_MASTER b
                              on 1=1
                              and a.DEVICE_ID = b.DEVICE_ID
                              WHERE a.DATA_ADDRESS IS NOT NULL 
                                    AND b.DEVICE_ID = '{0}'""".format(var)
                #rint(ora_sql1)
                addr=DeviceDic[var]
                if len(addr) == 1:
                    addr = '0'+addr
                
                fv = ora_db.execute(ora_sql1)
                for token in ora_db:
                    tag = token[0]
                    Device_addr = token[1]
                    FactoryID = token[2]
                    unit = token[3]
                    REGISTERS_INFO = Device_addr.split()
                    REGISTERS_INFO.insert(0, addr)
                    REGISTERS_INFOS.append(REGISTERS_INFO)                 
                    tagList[tag] = [REGISTERS_INFO, kind, FactoryID, unit]

        elif maker == 'VI' or maker == 'MI' or maker == 'ECO' or maker == 'LS':

            ora_sql = """SELECT DEVICE_ID, DEVICE_ADDRESS, DEVICE_USE FROM DEVICE_MASTER WHERE DEVICE_TYPE = 'E' AND USE_YN = 'Y' AND COM_CONNECT_DEVICE_ID = '{0}' AND MAKER LIKE '%{1}%'""".format(Device, maker)
            #ora_sql = """SELECT DEVICE_ID, DEVICE_ADDRESS FROM DEVICE_MASTER WHERE DEVICE_TYPE = 'E' AND USE_YN = 'Y' AND COM_CONNECT_DEVICE_ID = 'D00007' AND MAKER LIKE '%{1}%'""".format(Device, maker)
            #print(ora_sql)
            tfv = ora_db.execute(ora_sql)
            for token in ora_db:
               tmp_row=[]
               subId = token[0]
               Deviceaddr = token[1]
               kind = token[2]
               DeviceList.append(subId)
               DeviceDic[subId] = Deviceaddr
            for var in DeviceList:
                ora_sql1 = """SELECT a.TAG_ID, 
                                     a.DATA_ADDRESS, 
                                     b.FACTORY_ID, 
                                     a.CORRECT_RATIO 
                              FROM TAG_MASTER a
                              INNER JOIN DEVICE_MASTER B
                              ON 1=1
                              AND a.DEVICE_ID = b.DEVICE_ID
                              WHERE a.VIRTUAL_TAG_YN = 'N' 
                              AND a.COLL_YN = 'Y' 
                              AND a.DEVICE_ID = '{0}'""".format(var)
                addr=DeviceDic[var]
                if len(addr) == 1:
                    addr = '0'+addr

                fv = ora_db.execute(ora_sql1)
                for token in ora_db:
                    tag = token[0]
                    Device_addr = token[1]
                    FactoryID = token[2]
                    unit = token[3]
                    REGISTERS_INFO = Device_addr.split()
                    REGISTERS_INFO.insert(0, addr)
                    REGISTERS_INFOS.append(REGISTERS_INFO)
                    tagList[tag] = [REGISTERS_INFO, kind, FactoryID, unit]
        ora_db.close()
        ora_con.close()

        return REGISTERS_INFOS, tagList
    except Exception as Ex:
        log = osLogger.cOSLogger(_pPrefix='Exception', _pLevel='ERROR')
        log.writeLog(str(Ex))
        return None


def sendPkt(_pData: list = None, _pEndian: str = 'B', _pFunc: str = '03', _pMaker: str = None):
    try:
        #print(_pEndian, _pMaker)
        maker = _pMaker
        if _pData is None:
            raise Exception('[sendPkt] Invalid input : None type')

        if len(_pData) != 5:
            raise Exception('[sendPkt] Invalid data length : ' + str(len(_pData)))

        if str(type(_pData)) != str(type(list())):
            raise Exception('[sendPkt] Invalid data type : ' + str(type(_pData)))

        #print(_pData)
        Pkt = bytearray()

        if _pEndian == 'B':
            Pkt = bytes.fromhex(_pData[0]) + bytes.fromhex(_pFunc) + bytes.fromhex(_pData[1]) + bytes.fromhex(_pData[2]) + bytes.fromhex(_pData[3]) + bytes.fromhex(_pData[4])
        elif _pEndian == 'L':
            Pkt = bytes.fromhex(_pData[0]) + bytes.fromhex(_pFunc) + bytes.fromhex(_pData[2]) + bytes.fromhex(_pData[1]) + bytes.fromhex(_pData[4]) + bytes.fromhex(_pData[3])
        else:
            raise Exception('[sendPkt] Invalid endian type : \'' + _pEndian + '\'')

        CheckSum = osCRC.cCRC(Pkt, _pMode=16, _pMaker=maker).convertCRC16()
        return Pkt + CheckSum
    except Exception as Ex:
        log = osLogger.cOSLogger(_pPrefix='Exception', _pLevel='ERROR')
        log.writeLog(str(Ex))
        return None

def tag_split(tag):
    try:
        tagNm = tag.split('/')
        tagNm = tagNm[2].replace('.conf','')

        return tagNm
    except Exception as Ex:
        log = osLogger.cOSLogger(_pPrefix='Exception', _pLevel='ERROR')
        log.writeLog(str(Ex))
        return None


def JBmake_data( tag, data, time, kind, factoryID):
    tagNm = tag
    df = []
    try:
      totalvm = data[3]+data[4]+data[5]+data[6]
      totalvb = data[7]+data[8]+data[9]+data[10]
      temp = data[11]+data[12]+data[13]+data[14]
      press = data[15]+data[16]+data[17]+data[18]
      cf = data[27]+data[28]+data[29]+data[30]

      zb = data[31]+data[32]+data[33]+data[34]
 
      totalvm = struct.unpack('!L', bytes.fromhex(totalvm))
      totalvb = struct.unpack('!L', bytes.fromhex(totalvb))
      temp = struct.unpack('!f', bytes.fromhex(temp))
      press = struct.unpack('!f', bytes.fromhex(press))
      cf = struct.unpack('!f', bytes.fromhex(cf))
      zb = struct.unpack('!f', bytes.fromhex(zb))
      #df[tag] = {'tag' : tag, 'totalVm' : round(totalvm, 3), 'totalVb' : round(totalvb, 3), 'Temp' : round(temp, 3), 'Press' : round(press, 3), 'Cf' : round(cf, 3), 'Zb' : round(zb, 3), 'time' : time}
      #df[tag] = {'tag' : tag, 'totalVm' : totalvm[0], 'totalVb' : totalvb[0], 'Temp' : temp[0], 'Press' : press[0], 'Cf' : cf[0], 'Zb' : zb[0], 'time' : time}

      df = [tagNm+'_TOT-UNCORR-USAGE||'+str(totalvm[0])+'||'+time+'||'+kind+'||'+factoryID]
      df.append(tagNm+'_TOT-CORR-USAGE||'+ str(totalvb[0])+'||'+time+'||'+kind+'||'+factoryID)
      df.append(tagNm+'_AVG-TEMP||'+ str(temp[0])+'||'+time+'||'+kind+'||'+factoryID)
      df.append(tagNm+'_AVG-PRESS||'+ str(press[0])+'||'+time+'||'+kind+'||'+factoryID)
      df.append(tagNm+'_CORR-FACTOR||'+ str(cf[0])+'||'+time+'||'+kind+'||'+factoryID)
      #df.append(tagNm+'_Zb||'+ str(zb[0])+'||'+time+'||'+kind+'||'+factoryID)
    except:
      pass

    return df

def ELmake_data( tag, data, time, kind, factoryID):
    tagNm = tag
    df = []
    press = data[15]+data[16]+data[17]+data[18]
    temp = data[19]+data[20]+data[21]+data[22]
    totalvm = data[23]+data[24]+data[25]+data[26]
    totalvmf = data[27]+data[28]+data[29]+data[30]
    totalvb= data[31]+data[32]+data[33]+data[34]
    totalvbf = data[35]+data[36]+data[37]+data[38]
    cf = data[39]+data[40]+data[41]+data[42]

    press =  struct.unpack('!f', bytes.fromhex(press))
    temp =  struct.unpack('!f', bytes.fromhex(temp))
    totalvm = struct.unpack('!I', bytes.fromhex(totalvm))
    totalvmf =  struct.unpack('!f', bytes.fromhex(totalvmf))
    totalvb = struct.unpack('!I', bytes.fromhex(totalvb))
    totalvbf =  struct.unpack('!f', bytes.fromhex(totalvbf))
    cf = struct.unpack('!f', bytes.fromhex(cf))

    df = [tagNm+'_TOT-UNCORR-USAGE||'+str(totalvm[0]+totalvmf[0])+'||'+time+'||'+kind+'||'+factoryID]
    df.append(tagNm+'_TOT-CORR-USAGE||'+ str(totalvb[0]+totalvbf[0])+'||'+time+'||'+kind+'||'+factoryID)
    df.append(tagNm+'_AVG-TEMP||'+ str(temp[0])+'||'+time+'||'+kind+'||'+factoryID)
    df.append(tagNm+'_AVG-PRESS||'+ str(press[0])+'||'+time+'||'+kind+'||'+factoryID)
    df.append(tagNm+'_CORR-FACTOR||'+ str(cf[0])+'||'+time+'||'+kind+'||'+factoryID)

    return df

def LSmake_data(tag, data, time, dtype, low, kind, factoryID, unit: int = 1):
    df = []
     
    n_data = data[3]+data[4]+data[5]+data[6]
    n_data = struct.unpack('!f', bytes.fromhex(n_data))
    if unit == None:
      unit = 1

    if n_data[0] == None:
      pass
    elif math.isnan(n_data[0]):
      pass
    else:
      n_data = float(n_data[0]) * float(unit)
      df = [tag+'||'+str(n_data)+'||'+time+'||'+kind+'||'+factoryID]

    return df

def VImake_data(tag, data, time, dtype, low, kind, factoryID, unit: int = 1):
    df = []

    if unit is None:
      unit = 1
    if low == 'E2':
      n_data = data[9]+data[10]+data[7]+data[8]+data[5]+data[6]+data[3]+data[4]
      n_data = struct.unpack('!d', bytes.fromhex(n_data))
      n_data = float(n_data[0]) * float(unit)
      df = [tag+'||'+str(n_data)+'||'+time+'||'+kind+'||'+factoryID]
    else:
      n_data = data[5]+data[6]+data[3]+data[4]
      n_data = struct.unpack('!f', bytes.fromhex(n_data))
      n_data = float(n_data[0]) * float(unit)
      df = [tag+'||'+str(n_data)+'||'+time+'||'+kind+'||'+factoryID]
    return df


def ECOmake_data( tag, data, time, dtype, row, kind, factoryID, unit: int = 1):
    df = []
    ndata = 0

    if unit is None:
        unit = 1

    if dtype == '11':
        n_data = data[3]+data[4]+data[5]+data[6]
        n_data = struct.unpack('!f', bytes.fromhex(n_data))        
        n_data = float(n_data[0]) * float(unit)
        df = [tag+'||'+str(n_data)+'||'+time+'||'+kind+'||'+factoryID]
    elif dtype == '03':
        aa = '0000'
        n_data = aa+data[3]+data[4]
        n_data = struct.unpack('!i', bytes.fromhex(n_data))
        n_data = float(n_data[0]) * float(unit)          
        df = [tag+'||'+str(n_data)+'||'+time+'||'+kind+'||'+factoryID]
    elif dtype == '05':
        n_data = data[3]+data[4]+data[5]+data[6]
        n_data = struct.unpack('!i', bytes.fromhex(n_data))
        n_data = float(n_data[0]) * float(unit)
        df = [tag+'||'+str(n_data)+'||'+time+'||'+kind+'||'+factoryID]

    return df

def pkt_split( pkt ):
    pkt = pkt.replace("x","")
    pkt = pkt.split("\\")
    del pkt[0]

    return pkt

def kafka_connetion_info():
    ConInfoFile = open('/home/seah/app/bestFems/connect.conf', 'r', encoding='UTF8')
    Coninfo = ConInfoFile.read()
    ConInfoFile.close

    Conin = Coninfo.split(',')
    # Oracle info mapping
    ora_ip = Conin[0]
    ora_port = Conin[1]
    ora_user = Conin[2]
    ora_pw = Conin[3].rstrip('\n')
    ora_string = ora_user+'/'+ora_pw+'@'+ora_ip+':'+ora_port+'/BESTSF'
    ora_con = cx_Oracle.connect(ora_string)
    ora_db = ora_con.cursor()

    ora_sql = """ select A.IP_ADDRESS, A.PORT, mq.TOPIC FROM CONNECT_INFO a, MESSAGE_QUEUE mq where a.CONNECT_ID = 'KafkaCon001' AND a.CONNECT_ID = mq.CONNECT_ID
                  """
    tfv = ora_db.execute(ora_sql)
    kafka_ip = 0
    kafka_port = 0
    kafka_topic = 0
   
    for token in ora_db:
       kafka_ip = token[0]
       kafka_port = token[1]
       kafka_topic = token[2]

    return kafka_ip, kafka_port, kafka_topic 


def get_tag_ids():
    ConInfoFile = open('/home/seah/app/bestFems/connect.conf', 'r', encoding='UTF8')
    Coninfo = ConInfoFile.read()
    ConInfoFile.close

    Conin = Coninfo.split(',')
    # Oracle info mapping
    ora_ip = Conin[0]
    ora_port = Conin[1]
    ora_user = Conin[2]
    ora_pw = Conin[3].rstrip('\n')
    ora_string = ora_user+'/'+ora_pw+'@'+ora_ip+':'+ora_port+'/BESTSF'
    ora_con = cx_Oracle.connect(ora_string)
    ora_db = ora_con.cursor()

    ora_sql = """ select tag_id 
                  from tag_master
                  """
    tfv = ora_db.execute(ora_sql)
    tag_id_lists = []
    for row in ora_db :
      tag_id_lists.append(row[0])
    return tag_id_lists
  

def getResponse(DeviceInfo: dict = None, Device: str = None, ip_port: str = None , kafka_topic: str = None, tag_lists: str = None):
    init = common()
    init.my_conn('172.17.1.34', 3306, 'root', 'dt01@', 'fems')

    try:
        #print('process start')
        sleep(0.5)
        #init = common()
        #init.my_conn('172.17.1.34', 3306, 'root', 'dt01@', 'fems')
        #print('Connect to : ' + DeviceInfo[Device][0] + ':' + DeviceInfo[Device][1])
        maker = DeviceInfo[Device][3]
        REG_INFO, tagList = readDeviceInfo(Device, maker)

        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.settimeout(1.0)
        client.connect((DeviceInfo[Device][0], int(DeviceInfo[Device][1])))
        kafka_client = realtime(ip_port[0], ip_port[1] ) 

        result = list()
        #tagID = tagList[0].split("_")[0]
        #tag_type = tagList[1]
        ndata = 0
        for key, slaveInfo in tagList.items():
            Device_ID = key.split("_")[0]
            #DeviceUse
            tag_type = slaveInfo[1]
#        for slaveInfo in REG_INFO:
            if tag_type == '1':
                if maker == 'LS':
                    sPkt = sendPkt(slaveInfo[0], DeviceInfo[Device][2], '04')
                elif maker == 'MI':
                    sPkt = sendPkt(slaveInfo[0], DeviceInfo[Device][2], '03')
                elif maker == 'VI':
                    sPkt = sendPkt(slaveInfo[0], DeviceInfo[Device][2], '04')
                elif maker == 'ECO':
                    sPkt = sendPkt(slaveInfo[0], DeviceInfo[Device][2], '04', maker)
                else:
                    raise Exception('[run:getResponse] Invalid Maker : ' + str(maker))
            elif tag_type == '2' or '3':
                if maker == 'JB':
                    sPkt = sendPkt(slaveInfo[0], DeviceInfo[Device][2], '03')
                elif maker == 'EL':
                    sPkt = sendPkt(slaveInfo[0], DeviceInfo[Device][2], '03')
                else:
                    raise Exception('[run:getResponse] Invalid Maker : ' + str(maker))
            else:
                raise Exception('[run:getResponse] Invalid type : ' + str(tag_type))
            print('[' + maker + '] ['+Device_ID+'] Send (', str(len(sPkt)).rjust(3, ' '), ') : ', Global.byte2Hex(sPkt))
            try :
              client.send(sPkt)
              sleep(0.5)
              rPkt = client.recv(MAX_BUF)
              #CSum = osCRC.cCRC(Pkt, _pMode=16, _pMaker=maker).checkCRC_Eco(rPkt)
              pkt = pkt_split(Global.byte2Hex(rPkt))
              time = str(datetime.datetime.now())
              if rPkt is not None :
                  if maker == 'JB':
                      df = JBmake_data(Device_ID, pkt, time, tag_type, slaveInfo[2])
                  elif maker == 'EL':
                      df = ELmake_data(Device_ID, pkt, time, tag_type, slaveInfo[2])
                  elif maker == 'ECO' or maker == 'MI':
                      #if len(pkt) == 9:
                      df = ECOmake_data(key, pkt, time, slaveInfo[0][1], slaveInfo[0][2], tag_type, slaveInfo[2], slaveInfo[3])
                  elif maker == 'LS':
                      df = LSmake_data(key, pkt, time, slaveInfo[0][1], slaveInfo[0][2], tag_type, slaveInfo[2], slaveInfo[3])
                  elif maker == 'VI':
                      df = VImake_data(key, pkt, time, slaveInfo[0][1], slaveInfo[0][2], tag_type, slaveInfo[2], slaveInfo[3])
                  print(df)
   
                  # tag_master 와 매칭 작업 
                  temp_df = []
                  for row in df : 
                    value = row.split("||")
                    tag_id = value[0]
                    if tag_id in tag_lists:
                      temp_df.append( row )
                    else :
                      print('없는 태그 : ',  tag_id )
                  df = temp_df
                  # 매칭이 된 데이터만 카프카로 전송
                  #print(df)
                  if maker == 'ECO' :
                      if len(pkt) == 9 or len(pkt) == 14:
                        kafka_client.sendtokafka(kafka_topic, df)
                        #to_kafka(df)
                      else:
                          pass
                  else:
                      kafka_client.sendtokafka(kafka_topic, df)
                      #to_kafka(df)
                      #pass
              #if maker=='JB':
              #   sleep()
            except Exception as ex: 
              try :
                error_time = str(datetime.datetime.now())
                insert_id = str(uuid.uuid1())
                error_value =  { 'uuid' : insert_id, 'pro_id' : 'mm_00_collect.py', 'time' : error_time, 'Device_ID' : Device_ID, 'error_name' : str(ex) }

                error_sql = init.my_select('SP_GET_SQL', ('sql_06' ,))
                error_sql = error_sql[0][0]
                init.my_bulk_insert( error_sql, [list(error_value.values())] ) 
              except :
                print( 'log insert error' )
            sleep(0.5)
    except Exception as Ex:
        # mariaDB error insert
        error_time = str(datetime.datetime.now())
        insert_id = str(uuid.uuid1())
        error_value =  { 'uuid' : insert_id, 'pro_id' : 'mm_00_collect.py', 'time' : error_time, 'Device_ID' : Device, 'error_name' : str(Ex) }
        error_sql = init.my_select('SP_GET_SQL', ('sql_06' ,))
        error_sql = error_sql[0][0]
        init.my_bulk_insert( error_sql, [list(error_value.values())] )
    finally:
        client.close()
        #print('Closed')


        #log = osLogger.cOSLogger(_pPrefix='Exception', _pLevel='ERROR')
        #log.writeLog(str(Ex))
        #return None

def isValid(_pData=None):
    try:
        if _pData is None:
            raise Exception('[run:isValid] Invalid Input Data : None Type')

        if len(_pData) <= 2:
            raise Exception('[run:isValid] Invalid Input Data length : ' + str(len(_pData)))

        if str(type(_pData)) == str(type(str())):
            pass
        elif str(type(_pData)) == str(type(bytes())):
            calcVal = osCRC.cCRC(_pData[:-2], _pMode=16).convertCRC16()
            if calcVal == _pData[-2:]:
                return True
            else:
                logMsg = 'Rcv Packet : ' + Global.byte2Hex(_pData) + '\nRcv CRC : ' + str(_pData[-2:]) + '\nCalc CRC : ' + str(calcVal)
                log = osLogger.cOSLogger(_pPrefix='Exception', _pLevel='ERROR')
                log.writeLog(logMsg)
                return False
        else:
            raise Exception('[run:isValid] Invalid input data Type : ' + str(type(_pData)))
    except Exception as Ex:
        log = osLogger.cOSLogger(_pPrefix='Exception', _pLevel='ERROR')
        log.writeLog(str(Ex))
        return None


def run(kafka_client, kafka_topic, tag_lists):
    try:
        # arguments
        Sync = readArguments()

        # devices conf
        DeviceList = readDeviceConf(Maker)
        procs = []
        # 카프카 접속정보
        #kafka_ip, kafka_port, kafka_topic = kafka_connetion_info() 
        # 카프카 클라이언트 선언
        #kafka_client = realtime ( kafka_ip, kafka_port )
        if DeviceList is None:
            raise Exception('Fail to read Device Configuration file')
        
        # device info 
        for deviceInfo in DeviceList.keys():
            try:
              print(deviceInfo)
              #thread = threading.Thread(target=getResponse, name=deviceInfo, args=(DeviceList, deviceInfo, kafka_client, kafka_topic, tag_lists))
              #thread.start()
              #if Sync:
              #   thread.join()
              #else:
              #   pass
              proc = Process(target=getResponse, name=deviceInfo, args=(DeviceList, deviceInfo, kafka_client, kafka_topic, tag_lists))
              procs.append(proc)
              proc.start()
            except:
              pass
            #  print('----------------------re:'+deviceInfo)
            #  proc = Process(target=getResponse, name=deviceInfo, args=(DeviceList, deviceInfo, kafka_client, kafka_topic, tag_lists))
            #  procs.append(proc)
            #  proc.start()
            

        for proc in procs:
            proc.join()
            
        return True
    except Exception as Ex:
        log = osLogger.cOSLogger(_pPrefix='Exception', _pLevel='ERROR')
        log.writeLog(str(Ex))
        return False

if __name__ == '__main__':
    #makeDeamon()
    #log = osLogger.cOSLogger(_pPrefix='SERVER', _pLevel='INFO')
    #logMsg = 'Data Collector Server Start [' + str(os.getpid()) + ']'
    #log.writeLog(logMsg)
    runFlag = False

    # 카프카 접속정보
    kafka_ip, kafka_port, kafka_topic = kafka_connetion_info()
    # 카프카 클라이언트 선언
    kafka_client = ( kafka_ip, kafka_port )
    # tag_list 가지고 오기
    tag_lists = get_tag_ids()
    #print(Maker)
    while True:
      #print(datetime.datetime.now().second)
      #if Maker == 'JB':
      #  if datetime.datetime.now().minute % 1 == 0:
      #     #print('True')
      #     runFlag = True
      #  else:
      #     runFlag = False
      #else:
      if Maker == 'VI':
         if datetime.datetime.now().second == 45:
            runFlag = True
         else:
            runFlag = False
      else:
         if datetime.datetime.now().second == 0:
            #print('True')
            runFlag = True
         else:
            runFlag = False

      if runFlag:
         run( kafka_client, kafka_topic, tag_lists )

         # Garbage delete
         gc.collect()
         runFlag = False
         #if Maker == 'JB':
         #  #print('sleep')
         #   sleep(100)
         #else:
         #  sleep(5)
      else:
         sleep(1)
