import pymongo
import datetime
from dateutil.relativedelta import relativedelta
from common import common
import re
import sys
import subprocess
def cal_val ( mongoTime, currentTime ) :
  time1 = currentTime
  time2 =  mongoTime
  minus_time = time1 - time2
  cal_time = minus_time.total_seconds()
  return cal_time


if __name__ == "__main__":
  # 가상 태그의 파라미터 
  #from_time = '2020-02-01 00:07:00'
  #to_time = '2020-02-02 00:08:00'

  init = common() 
  param = sys.argv[1]
  from_time = sys.argv[2]
  to_time = sys.argv[3]
  if param == 'minute' :
    pro_exec = "python3.6 /home/seah/app/bestFems/mm_246_summary.py minute '{0}'" 
    #pro_exec = "python3.6 /home/seah/app/bestFems/mm_11_summary.py minute '{0}'"
    jump_time = 15
  elif param == 'hour' :
    pro_exec = "python3.6 /home/seah/app/bestFems/mm_246_summary.py hour '{0}'"
    jump_time = 60
  elif param == 'day' :
    pro_exec = "python3.6 /home/seah/app/bestFems/mm_246_summary.py day '{0}'"
    jump_time =  60 * 24
  elif param == 'peek':
    #pro_exec = "python3.6 /home/seah/app/bestFems/mm_variable_05_peek_data.py '{0}'"
    pro_exec = "python3.6 /home/seah/app/bestFems/mm_266_peek.py '{0}'"
    jump_time = 15
  elif param == 'vtag':
    pro_exec = "python3.6 /home/seah/app/bestFems/mm_999_vtag_1m_use.py vtag1 '{0}'"
    jump_time = 1
  elif param == 'use':
    pro_exec = "python3.6 /home/seah/app/bestFems/mm_226_1m_use.py use '{0}'"
    jump_time = 1
  elif param == 'real_vtag':
    pro_exec = "python3.6 /home/seah/app/bestFems/mm_234_vtag_1m_use.py vtag1 '{0}'"
    jump_time = 1
  elif param == 'use_02':
    pro_exec = "python3.6 /home/seah/app/bestFems/mm_996_1m_use.py use '{0}'"
    jump_time = 1
  elif param == 'basic':
    pro_exec = "python3.6 /home/seah/app/bestFems/mm_28_basic_unit.py '{0}'"
    jump_time = 1 * 60 * 24
 



  else :
    exit()

  # 가상 태그 시간을 계산한다. 
  minute_value = int( ( cal_val( init.str_to_time( from_time ), init.str_to_time( to_time ) ) ) / 60 ) 
  print( minute_value ) 
 
  # 시작 시간 1분 단위로 만들기 
  from_time_list = []
  for idx in range( 0, int(minute_value), jump_time ):
    token_time = init.str_to_time( from_time ) +  datetime.timedelta(minutes = idx)
    from_time_list.append( str(token_time) )

  print( from_time_list )

  import time 
  time.sleep( 0.1 ) 


  for time_param in from_time_list:
    try : 
      m_command = pro_exec.format( time_param ) 
      a = subprocess.check_output( m_command, shell = True)
      print( a ) 
    except : 
      print( m_command )
 
  exit() 
 
