#!/usr/bin/env python
# coding: utf-8

# # 라이브러리 선언

# In[6]:


# pandas : 데이터 조작, 분석 라이브러리
import pandas as pd

# os : 운영체제와 상호작용하기 위한 기능 제공
import os

# sqlalchemy : 파이썬으로 sql db 다루기 위한 라이브러리
import sqlalchemy as sa

# sys : 내장 모듈 중 하나로, 시스템과 관련된 작업을 수행
# argparse에 비해 간단한 스크립트이거나 명령행 인수가 매우 간단한 경우에 적합
import sys


# # 사용된 함수

# In[7]:


def set_engine(category, row):
    """
    db에 접속하는 엔진 설정
    (주의!)db종류 추가 될 경우 prefix 추가
    
    Args:
    - category : db종류
    - row : df 행의 값
    
    Returns:
    -  engine
    """
    if category == '0' :
        dbPrefix = "oracle+cx_oracle"
    elif category == '1' :
        dbPrefix = "mysql+pymysql"
    elif category == '2' :
        dbPrefix = "postgresql"
    dbIp = row.DB_IP
    dbId = row.DB_ID
    dbPw = row.DB_PW
    dbPort = row.DB_PORT
    dbName = row.DB_NAME
    engine = sa.create_engine(f"{dbPrefix}://{dbId}:{dbPw}@{dbIp}:{dbPort}/{dbName}")
    return engine


# In[ ]:


def pretreatment(pt):
    """
    데이터 전처리
    
    Args:
    - pt : 전처리 대상 df
    """   
    for column in pt.columns:
        pt.columns.astype(str)
    
    for index, row in pt.iterrows():
        row.USE_YN = row.USE_YN.lower()
        row.TABLE_LOC = row.TABLE_LOC.lower()
    return pt


# In[8]:


def table_exists(table_name, engine):
    """
    테이블 존재 유무확인
    
    Args:
    - table_name : 확인할 테이블 이름
    - engine
    """    
    inspector = sa.inspect(engine)
    return table_name in inspector.get_table_names()


# # 매개변수 받기(파일이름, 확장자 or 테이블 이름)

# In[9]:


#python파일 실행할 때 파일이름을 매개변수로 받아서 해당 파일을 사용
if len(sys.argv) < 2:
    print("파라미터 테이블 이름 혹은 파라미터 테이블 파일 이름을 입력해주세요.")
    # 파이썬 프로그램을 즉시 종료
    sys.exit()

inputParam = sys.argv[1]
isFile = 0 #파일이 있을 때 1

# 파라미터 테이블 파일이 없고 파라미터 테이블이 DB에 존재할 때
if len(inputParam.split('.')) == 1:
    tableName = inputParam
    print(f"테이블이름 : {tableName}")

    # 파라미터 테이블이 저장된 DB 정보
    # 매개변수로 입력받게 수정가능
    data = {
        'USE_YN': [None],
        'TABLE_LOC': ['y'],
        'CATEGORY': ['0'],
        'DB_IP': ['192.168.110.112'],
        'DB_ID': ['kopo'],
        'DB_PW': ['kopo'],
        'DB_PORT': ['1521'],
        'DB_NAME': ['orcl'],
        'FOLDER_PATH': [None]
    }
    locDf = pd.DataFrame(data)
    
# 파라미터 테이블 파일이 있을 때
else :
    isFile = 1
    inputFileName = inputParam.split('.')[0]
    inputFileExtension = inputParam.split('.')[1]
    print(f"파일이름 : {inputFileName}, 파일확장자 : {inputFileExtension}")


# In[ ]:


if isFile == 0:
    for index, row in locDf.iterrows():
        engine = set_engine(row.CATEGORY, row)
    
    # 테이블 존재하면 select, 없으면 create
    if table_exists(tableName, engine):
        selquery = sa.text("SELECT * FROM {}".format(tableName))
        pt = pd.read_sql_query(selquery, engine)

        # csv파일이 저장될 DB정보
        useDf = pt[pt['USE_YN'] == 'y'].copy()
    else:
        print(f"{tableName}이 존재하지 않아서 생성합니다.")
        pt.to_sql(name=tableName, con=engine, if_exists="replace", index=False)
        print(f"{tableName}이 생성 완료.")


# In[ ]:


if isFile == 1:
    if inputFileExtension == 'xlsx':
        pt = pd.read_excel("./" + inputFileName + "." + inputFileExtension, engine="openpyxl")
    elif inputFileExtension == 'csv':
        pt = pd.read_csv("./" + inputFileName + "." + inputFileExtension)

    # 전처리
    pt = pretreatment(pt)
        
    # 파라미터테이블이 저장될 DB정보
    locDf = pt[pt['TABLE_LOC'] == 'y'].copy()
    
    # csv파일이 저장될 DB정보
    useDf = pt[pt['USE_YN'] == 'y'].copy()


# # csv파일 DB에 저장하기

# In[ ]:


for index, row in useDf.iterrows():
    engine = set_engine(row.CATEGORY, row)
    folderPath = row.FOLDER_PATH.replace(",",".") + "/"
    fileList = os.listdir(folderPath)
    
    # 파일리스트가 비어있을 때 예외처리
    if not fileList:
        #rasie : 예외를 명시적으로 발생시키는 데 사용
        raise FileNotFoundError("FOLDER_PATH 값을 확인하거나 폴더에 파일이 있는 지 확인하세요.")

    # 파일리스트 순회하면서 인코딩하고 create table
    for file in fileList:
        filePath = folderPath + file
        fileName = file.split('.')[0]
        tableName = fileName+"_kdj"
        encodings = ['utf-8', 'cp949', 'ms949', 'euc-kr']
        for encoding in encodings:
            try:
                indata = pd.read_csv(filePath, encoding = encoding)
                print(f"{file} 인코딩({encoding}) 성공.")
                break
            except UnicodeDecodeError:
                print(f"{file} 인코딩({encoding}) 실패.")
        else:
            raise Exception("모든 인코딩 실패")

        floatColumns = list(indata.columns[indata.dtypes == 'float'])
        floatLen = 200
        typeDict={}
        for i in range(0, len(floatColumns)):
            typeDict[ floatColumns[i] ] = sa.types.VARCHAR(floatLen)
        indata.to_sql(name=tableName, if_exists="replace", con=engine, dtype=typeDict, index=False)
        print(f"{tableName} 테이블 생성 성공.")

