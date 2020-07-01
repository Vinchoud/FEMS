#!/usr/bin/env python3
# -*-coding:utf-8-*-

# =======================================================================================================================
# 이 모듈은 CRC-16을 계산하고 반환하는 기능 수행
# 작성자 : 이현신
# =======================================================================================================================


import numpy as np
import osLogger
import Global

from pymodbus.compat import byte2int


class cCRC:
    _mCRC = str()
    _mCRC_Table = None

    # CRC 클래스 생성자
    def __init__(self, _pData: str = None, _pType: str = None, _pMode: int = 8, _pMaker: str = None):
        try:
            if len(_pData) == 0:
                raise Exception('[CRC] None Data is not supported')
            #print(_pData, _pType, _pMode, _pMaker)

            if _pMode == 8:
                if _pType is None:
                    if str(type(_pData)) != str(type(str())):
                        raise Exception('[CRC] The type of input data must be a string')

                    chk1, chk2 = self.calcCRC(_pData)
                    self._mCRC = chr(chk1) + chr(chk2)
                elif _pType == 'b':
                    chk1, chk2 = self.calcCRC2(_pData)
                    self._mCRC = chr(chk1) + chr(chk2)
            else:
                self.initTable()
                #print(_pMaker)
                if _pMaker is None:
                    # Modbus CRC 16
                    self._mCRC = self.calcCRC16(_pData)
                else:
                    self._mCRC = self.calcCRC_Eco(_pData)
        except Exception as Ex:
            log = osLogger.cOSLogger(_pPrefix='Exception', _pLevel='ERROR')
            log.writeLog(str(Ex))

    # CRC 계산함수
    def calcCRC(self, _pData: str = None):
        try:
            if _pData is None:
                raise Exception('[CRC] None data is not supported')

            iLen = len(_pData)
            CharSum = 0
            for i in range(0, iLen):
                CharSum = np.uint8(np.uint8(CharSum) + np.uint8(ord(_pData[i])))
            CheckSum1 = ((CharSum & 0xF0) >> 4) + 0x30
            CheckSum2 = ((CharSum & 0x0F) >> 0) + 0x30
            return CheckSum1, CheckSum2
        except Exception as Ex:
            log = osLogger.cOSLogger(_pPrefix='Exception', _pLevel='ERROR')
            log.writeLog(str(Ex))
            return None, None

    def calcCRC2(self, _pData: bytearray = None):
        try:
            if _pData is None:
                raise Exception('[CRC] None data is not supported')

            iLen = len(_pData)
            CharSum = 0
            for i in range(0, iLen):
                CharSum = np.uint8(np.uint8(CharSum) + np.uint8(_pData[i]))

            CheckSum1 = np.uint8(((CharSum & 0xF0) >> 4) + 0x30)
            CheckSum2 = np.uint8(((CharSum & 0x0F) >> 0) + 0x30)
            return CheckSum1, CheckSum2
        except Exception as Ex:
            log = osLogger.cOSLogger(_pPrefix='Exception', _pLevel='ERROR')
            log.writeLog(str(Ex))
            return None, None

    # CRC 계산값 반환
    def getCRC(self):
        return self._mCRC

    # CRC16 Table 생성
    def initTable(self):
        try:
            result = []
            for byte in range(256):
                crc = 0x0000
                for _ in range(8):
                    if (byte ^ crc) & 0x0001:
                        crc = (crc >> 1) ^ 0xa001
                    else:
                        crc >>= 1
                    byte >>= 1
                result.append(crc)
            self._mCRC_Table = result
        except Exception as Ex:
            log = osLogger.cOSLogger(_pPrefix='Exception', _pLevel='ERROR')
            log.writeLog(str(Ex))

    def calcCRC16(self, _pData: bytearray = None):
        try:
            print('ModbusCRC')
            crc = 0xffff
            for a in _pData:
                idx = self._mCRC_Table[(crc ^ byte2int(a)) & 0xff]
                crc = ((crc >> 8) & 0xff) ^ idx
            swapped = ((crc << 8) & 0xff00) | ((crc >> 8) & 0x00ff)
            return swapped
        except Exception as Ex:
            log = osLogger.cOSLogger(_pPrefix='Exception', _pLevel='ERROR')
            log.writeLog(str(Ex))

    def calcCRC_Eco(self, _pData: bytearray = None):
        try:
            #print('ECOCRC')
            uchCRCHi = 0xFF
            uchCRCLo = 0xFF

            for a in _pData:
                idx = np.uint8(uchCRCHi) ^ np.uint8((byte2int(a)))
                uchCRCHi = np.uint8(uchCRCLo) ^ (self._mCRC_Table[idx] & 0xFF)
                uchCRCLo = np.uint8((self._mCRC_Table[idx] >> 8) & 0xFF)

            return (uchCRCHi << 8) | uchCRCLo
        except Exception as Ex:
            log = osLogger.cOSLogger(_pPrefix='Exception', _pLevel='ERROR')
            log.writeLog(str(Ex))

    def checkCRC_Eco(self, _pData: bytearray = None):
        try:
            crcCnt = len(_pData) -2 
            print(crcCnt)
        except Exception as Ex:
            log = osLogger.cOSLogger(_pPrefix='Exception', _pLevel='ERROR')
            log.writeLog(str(Ex))

    def convertCRC16(self):
        try:
            hexVal = '{:04x}'.format(self._mCRC)
            hiVal = hexVal[:2]
            lowVal = hexVal[-2:]
            retVal = bytes.fromhex(hiVal) + bytes.fromhex(lowVal)
            #retVal = bytes.fromhex(lowVal) + bytes.fromhex(hiVal)
            return retVal
        except Exception as Ex:
            log = osLogger.cOSLogger(_pPrefix='Exception', _pLevel='ERROR')
            log.writeLog(str(Ex))


if __name__ == '__main__':
    sndPkt = b'\x01\x04\x11\x02\x00\x02'
    srcPkt = b'\x01\x03\x1f\x40\x00\x10\x42\x06'
    crc = cCRC(_pData=sndPkt, _pMode=16, _pMaker='ECOSENSE')
    print('Send : ', Global.byte2Hex(sndPkt + crc.convertCRC16()))
    print('Source : ', Global.byte2Hex(srcPkt))
