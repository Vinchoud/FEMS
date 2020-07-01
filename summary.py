import opcua
import time
import pymongo
import datetime
import re
import time
import cx_Oracle

from common import common


class summary:
    def __init__(self):
        pass

    def min_return(self, data1, data2):
        #print(data1, data2)
        data1 = float(data1)
        data2 = float(data2)
        if data1< data2:
            return data1
        else:
            return data2

    def max_return(self, data1, data2):
        data1 = float(data1)
        data2 = float(data2)
        if data1 < data2:
            return data2
        else:
            return data1

    def sum_return(self, data1, data2):
        data1 = float(data1)
        data2 = float(data2)
        data1 += data2
        return round(data1, 3)

    def wight_time(self, p_from_time, p_to_time, p_seconds=1):
        init = common()
        p_to_time = init.str_to_time(p_to_time)
        p_from_time = init.str_to_time(p_from_time)
        temp_minus_time = p_to_time - p_from_time
        w_time = (temp_minus_time.total_seconds() / 60) * p_seconds
        return w_time

    def get_data(self, p_from_date, p_to_date, interval):
        init = common()
        conn = pymongo.MongoClient('172.17.1.34', 17001)
 
        interval = 'FEMS_'+interval+'_DATA'
        BigDataCollector = conn[interval]
        Cache = BigDataCollector['in_data']

        virtual_min_value = {}
        tag_list = []
        result_data = []
        result_summary = {}
        cnt = 0
        print('summary')
        for row in Cache.find({'time': {'$gte':p_from_date,'$lte': p_to_date}}).sort([("time", pymongo.ASCENDING)]):
            if row['tag'] not in result_summary:
               result_summary[row['tag']] = {'tag':row['tag'],'min': row['val'], 'max': row['val'], 'sum': float(row['val']), 'ave': row['val'], 'cnt':1, 'from_date':p_from_date, 'to_date':p_to_date}
               tag_list.append(row['tag'])
            else:
               #min
               result_summary[row['tag']]['min'] = self.min_return(result_summary[row['tag']]['min'], row['val'])
               #max
               result_summary[row['tag']]['max'] = self.max_return(result_summary[row['tag']]['max'], row['val'])
               #sume
               result_summary[row['tag']]['sum'] = self.sum_return(result_summary[row['tag']]['sum'], row['val'])
               result_summary[row['tag']]['cnt'] += 1


            #w_time = self.wight_time( virtual_min_value[row['tag']]['time'], row['time'] )
            #w_val = float(virtual_min_value[row['tag']]['val']) * w_time
            #virtual_min_value[row['tag']]['w_val'] = w_val

        for value in tag_list:
            result_summary[value]['ave'] = round(result_summary[value]['sum'] / result_summary[value]['cnt'], 3)

        if len(result_summary) == 0:
            pass
        else:
            exec_log = BigDataCollector['exec_log']
            #print(p_to_date)
            #exit()
            exec_log.delete_many({})
            exec_log.insert_one({'cur_date' : p_to_date})
            Cache.remove({'time': {'$gte':p_from_date,'$lte':p_to_date}}) 

        return result_summary, tag_list

    def get_hmdata(self, p_from_date, p_to_date, sp_parm, interval):
        
#        p_to_date = '2020-01-03 05:45:00'
        init = common()
        init.my_database_conn()
        from_date1 = ''
        print(sp_parm) 
        if sp_parm == '3':
           var = '5'
           if p_to_date < p_to_date[:10]+' 06:00:00':
              to_date = init.str_to_time(p_to_date) - datetime.timedelta(days=1)
              to_date = init.time_to_str(to_date)
              from_date = to_date[:10]+' 06:00:00'
              from_date1 = from_date
           else:
              to_date = p_to_date
              from_date = to_date[:10]+' 06:00:00'
              from_date1 = from_date
#              from_date1 = '2020-01-02 06:00:00'
        elif sp_parm == '4':
           var = '5'
           if p_to_date < p_to_date[:10]+' 06:00:00':
              to_date = init.str_to_time(p_to_date) - datetime.timedelta(days=30)
              to_date = init.time_to_str(to_date)
              from_date = to_date[:8]+'01 06:00:00'
           else:
              to_date = p_to_date
              from_date = to_date[:8]+'01 06:00:00'
              from_date1 = '2020-01-01 06:00:00'
        print(from_date1, p_to_date)
    
        col_info = init.my_table_select('SP_GET_SUM_INTERVAL', 'MongoCon001', var)
        colNm = col_info[4]
        print(colNm)
        # 저장서버 MongoDB 저장
        mongo_addr = init.my_table_select('SP_GET_SAVE_INFO', 'MongoCon001', '')
        mongo_ip = mongo_addr[1]
        mongo_port = int(mongo_addr[2])
        con = pymongo.MongoClient(mongo_ip, mongo_port)
        db = con["SEAH_FEMS"]
        col = db[colNm]
        tag_list = []
        result_summary = {}
        for row in col.find({'from_date':{'$gte':from_date1, '$lt':p_to_date}}).sort([("from_date", pymongo.ASCENDING)]):
            if row['tag'] not in result_summary:
               #print(row['acc_value'])
               result_summary[row['tag']] = {'tag':row['tag'], 'value':row['acc_value'],'min':row['acc_value'], 'max':row['acc_value'], 'from_date':from_date, 'to_date':to_date, 'kind':row['kind'], 'factory_id':row['factory_id']}
               tag_list.append(row['tag'])
            else:
               #print(row['acc_value'])
               result_summary[row['tag']]['min'] = self.min_return(result_summary[row['tag']]['min'], row['acc_value'])
               
               #max
               result_summary[row['tag']]['max'] = self.max_return(result_summary[row['tag']]['max'], row['acc_value'])
        format_date = p_to_date[:10].replace('-','')
        tmp_date = p_to_date[11:17].replace(':','')
        format_date = format_date+tmp_date

        try:
            cx_con = cx_Oracle.connect(init.string)
            cur = cx_con.cursor()
            for var in tag_list:
                code = 'E00002'
                if result_summary[var]['kind'] == '1':
                    code = 'E00001'
                elif result_summary[var]['kind'] == '3':
                    code = 'E00003'
                else:
                    pass
                
                result_summary[var]['value'] = float(result_summary[var]['max']) - float(result_summary[var]['min'])
                result_summary[var]['acc_value'] = result_summary[var]['max']
                val = float(result_summary[var]['value'])

                query = """select FN_ENG_CONVERSION('MJ', '{0}', '{1}', '{2}') AS "MJ",
                                  FN_ENG_CONVERSION('TOE', '{0}', '{1}', '{2}') AS "TOE",
                                  FN_ENG_CONVERSION('GHG', '{0}', '{1}', '{2}') AS "TCO2",
                                  FN_COST_CONVERSION('COST', '{0}', '{1}', '{2}', '{2}') AS "COST" FROM DUAL""".format(code, format_date, val)
                cur.execute(query)
                res = cur.fetchall()
                result_summary[var]['tj'] = res[0][0]
                result_summary[var]['toe'] = res[0][1]
                result_summary[var]['tco2'] = res[0][2]
                result_summary[var]['cost'] = res[0][3]
            cur.close()
            cx_con.close()
        except:
            result = "error : "
        #if sp_parm == '4':
        #    tag_list.remove('E00022_Total Active Energy') 
        if len(result_summary) == 0:
            pass
        else:
            lconn = pymongo.MongoClient('172.17.1.35', 17001)
            db = 'SEAH_FEMS'
            BigDataCollector = lconn[db]
            if sp_parm == '3':
               exec_log = BigDataCollector['TAG_SUM_TIME_29_DAY']
            elif sp_parm == '4':
               exec_log = BigDataCollector['TAG_SUM_TIME_29_MONTH']
            exec_log.delete_many({'from_date':{'$gte':from_date, '$lt':p_to_date}})
        print(result_summary)
        #print(tag_list)
        return result_summary, tag_list

    def get_exdata(self, p_from_date, p_to_date, sp_parm, interval):
        #p_from_date = '2020-01-09 14:00:00'
        #p_to_date = '2020-01-09 15:00:00'
        init = common()
        init.my_database_conn()

        if sp_parm == '2':
           var = '5'
        elif sp_parm == '3':
           var = '2'
           from_date = p_from_date[:10]
           from_date = from_date + ' 06:00:00'
           

        col_info = init.my_table_select('SP_GET_SUM_INTERVAL', 'MongoCon001', var)
        colNm = col_info[4]

        # 저장서버 MongoDB 저장
        mongo_addr = init.my_table_select('SP_GET_SAVE_INFO', 'MongoCon001', '')
        mongo_ip = mongo_addr[1]
        mongo_port = int(mongo_addr[2])

        con = pymongo.MongoClient(mongo_ip, mongo_port)
        db = con["SEAH_FEMS"]
        col = db[colNm]
        tag_list = []
        result_summary = {}

        print(colNm)
        print(p_from_date, p_to_date)
        for row in col.find({'from_date':{'$gte':p_from_date, '$lt' : p_to_date}}).sort([("from_date", pymongo.ASCENDING)]):
            if row['tag'] not in result_summary:
               result_summary[row['tag']] = {'tag':row['tag'], 'value':row['acc_value'],'min':row['acc_value'], 'max':row['acc_value'], 'from_date':p_from_date, 'to_date':p_to_date, 'kind':row['kind'], 'factory_id':row['factory_id']}
               tag_list.append(row['tag'])
            else:
               result_summary[row['tag']]['min'] = self.min_return(result_summary[row['tag']]['min'], row['acc_value'])
               #max
               result_summary[row['tag']]['max'] = self.max_return(result_summary[row['tag']]['max'], row['acc_value'])

        
        format_date = p_to_date[:10].replace('-','')
        tmp_date = p_to_date[11:17].replace(':','')
        format_date = format_date+tmp_date
        
        try:
            cx_con = cx_Oracle.connect(init.string)
            cur = cx_con.cursor()
            for var in tag_list:
                code = 'E00002'
                if result_summary[var]['kind'] == '1':
                    code = 'E00001'
                elif result_summary[var]['kind'] == '3':
                    code = 'E00003'
                else:
                    pass
                result_summary[var]['value'] = float(result_summary[var]['max']) - float(result_summary[var]['min'])
                result_summary[var]['acc_value'] = result_summary[var]['max']

                val = float(result_summary[var]['value'])

                query = """select FN_ENG_CONVERSION('MJ', '{0}', '{1}', '{2}') AS "MJ",
                                  FN_ENG_CONVERSION('TOE', '{0}', '{1}', '{2}') AS "TOE",
                                  FN_ENG_CONVERSION('GHG', '{0}', '{1}', '{2}') AS "TCO2",
                                  FN_COST_CONVERSION('COST', '{0}', '{1}', '{2}', '{2}') AS "COST" FROM DUAL""".format(code, format_date, val)                                               

                cur.execute(query)
                res = cur.fetchall()
                result_summary[var]['tj'] = res[0][0]
                result_summary[var]['toe'] = res[0][1]
                result_summary[var]['tco2'] = res[0][2]
                result_summary[var]['cost'] = res[0][3]

            cur.close()
            cx_con.close()
        except:
            result = "error : "
        #tag_list.remove('E00022_Total Active Energy')
        print(result_summary)

        if len(result_summary) == 0:
            pass
        else:
            lconn = pymongo.MongoClient('172.17.1.34', 17001)
            db = 'FEMS_ECO_15M_DATA'
            BigDataCollector = lconn[db]
            if sp_parm == '2':
                exec_log = BigDataCollector['h_exec_log']
            elif sp_parm == '3':
                exec_log = BigDataCollector['m_exec_log']
            exec_log.delete_many({})
            exec_log.insert_one({'cur_date' : p_to_date})

        return result_summary, tag_list
 
    def get_ecodata(self, p_from_date, p_to_date, interval):
        init = common()
        init.my_database_conn()

        conn = pymongo.MongoClient('172.17.1.34', 17001)
        tmp = interval
        db = 'FEMS_ECO_15M_DATA'
        BigDataCollector = conn[db]
        Cache = BigDataCollector['in_data']
        virtual_min_value = {}
        tag_list = []
        result_data = []
        result_summary = {}
        cnt = 0
        #유량계 계산식
        rgx = re.compile('TOT-CORR-USAGE', re.IGNORECASE)
        print('summary')
        for row in Cache.find({'tag':rgx,'time': {'$gte':p_from_date,'$lt': p_to_date}}).sort([("time", pymongo.ASCENDING)]):
            if row['tag'] not in result_summary:
               result_summary[row['tag']] = {'tag':row['tag'], 'min':row['val'], 'value':row['val'], 'acc_value' : row['val'], 'from_date':p_from_date, 'to_date':p_to_date, 'kind':row['kind'], 'factory_id':row['factory_id']}
               tag_list.append(row['tag'])
            else:
               #min
               result_summary[row['tag']]['min'] = self.min_return(result_summary[row['tag']]['min'], row['val'])
               #max
               result_summary[row['tag']]['acc_value'] = self.max_return(result_summary[row['tag']]['acc_value'], row['val'])
            
        format_date = p_to_date[:10].replace('-','')
        tmp_date = p_to_date[11:17].replace(':','')
        format_date = format_date+tmp_date


        #전력량계(ECO) 계산식
        rgx0 = re.compile('_Total Active Energy.*', re.IGNORECASE)
        for row in Cache.find({'tag':rgx0,'time': {'$gte':p_from_date,'$lt': p_to_date}}).sort([("time", pymongo.ASCENDING)]):
            if row['tag'] not in result_summary:
               result_summary[row['tag']] = {'tag':row['tag'], 'min':row['val'], 'value':row['val'], 'acc_value' : row['val'], 'from_date':p_from_date, 'to_date':p_to_date, 'kind':row['kind'], 'factory_id':row['factory_id']}
               tag_list.append(row['tag'])
            else:
               #min
               #f float(row['val']) > 10.0:
               result_summary[row['tag']]['min'] = self.min_return(result_summary[row['tag']]['min'], row['val'])
               #max
               result_summary[row['tag']]['acc_value'] = self.max_return(result_summary[row['tag']]['acc_value'], row['val']) 

        try:
            cx_con = cx_Oracle.connect(init.string)
            cur = cx_con.cursor()
            for var in tag_list:
                code = 'E00002'
                if result_summary[var]['kind'] == '1':
                    code = 'E00001'
                elif result_summary[var]['kind'] == '3':
                    code = 'E00003'
                else:
                    pass

                result_summary[var]['value'] = float(result_summary[var]['acc_value']) - float(result_summary[var]['min'])
                val = float(result_summary[var]['value'])

                query = """select FN_ENG_CONVERSION('MJ', '{0}', '{1}', '{2}') AS "MJ",
                                  FN_ENG_CONVERSION('TOE', '{0}', '{1}', '{2}') AS "TOE",
                                  FN_ENG_CONVERSION('GHG', '{0}', '{1}', '{2}') AS "TCO2",
                                  FN_COST_CONVERSION('COST', '{0}', '{1}', '{2}', '{2}') AS "COST" FROM DUAL""".format(code, format_date, val)
                cur.execute(query)
                res = cur.fetchall()
                result_summary[var]['tj'] = res[0][0]
                result_summary[var]['toe'] = res[0][1]
                result_summary[var]['tco2'] = res[0][2]
                result_summary[var]['cost'] = res[0][3]
                result_summary[var]['lod_tmzon'] = 'L'

            cur.close()
            cx_con.close()

        except:
            result = "error : "
        #print(tag_list)
        #print(result_summary)
        try:
          tag_list.remove('E00018_Total Active Energy High')
        except:
          pass
        try:
          tag_list.remove('E00018_Total Active Energy Low')
        except:
          pass
        try:
          tag_list.remove('E00019_Total Active Energy High')
        except:
          pass
        try:
          tag_list.remove('E00019_Total Active Energy Low')
        except:
          pass
        #exit()
        if len(result_summary) == 0:
            pass
        else:
            exec_log = BigDataCollector['exec_log']
            exec_log.delete_many({})
            exec_log.insert_one({'cur_date' : p_to_date})
            Cache.remove({'time': {'$gte':p_from_date,'$lte':p_to_date}})    

        return result_summary, tag_list
