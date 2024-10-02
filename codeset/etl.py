#!/usr/bin/env python
# coding: utf-8

# # 라이브러리 선언

# In[1]:


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

# In[2]:


def makeExParameterFile():
    data = {
        'use_yn': ['n', 'n', 'n', 'n'],
        'table_loc': ['n', 'n', 'n', 'y'],
        'category': ['0 : Oracle', '1 : mariaDB', '2 : postgre', None],
        'db_ip': ['kopo', None, None, None],
        'db_id': ['kopo', None, None, None],
        'db_pw': ['1521', None, None, None],
        'db_port': [None, None, None, None],
        'db_name': [None, None, None, None],
        'folder_path': ['Enter the folder path where the csv file is located.', None, None, None]
    }
    locDf = pd.DataFrame(data)
    
    # 폴더 생성
    folder_name = "../param"
    os.makedirs(folder_name, exist_ok=True)
    
    # CSV 파일 경로 설정
    file_path = os.path.join(folder_name, "prameter_table_ex.csv")
    
    # DataFrame을 CSV 파일로 저장
    test = locDf.to_csv(file_path, index=False)
    
    print(f"{folder_name}에 prameter_table_ex.csv 가 생성되었습니다.")


# In[23]:


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
    if category == "0" :
        dbPrefix = "oracle+cx_oracle"
    elif category == "1" :
        dbPrefix = "mysql+pymysql"
    elif category == "2" :
        dbPrefix = "postgresql"
    dbIp = row.db_ip
    dbId = row.db_id
    dbPw = row.db_pw
    dbPort = row.db_port
    dbName = row.db_name
    engine = sa.create_engine(f"{dbPrefix}://{dbId}:{dbPw}@{dbIp}:{dbPort}/{dbName}")
    return engine


# In[4]:


def pretreatment(pt):
    """
    데이터 전처리
    
    Args:
    - pt : 전처리 대상 df
    """   
    for column in pt.columns:
        pt[column] = pt[column].astype(str)
    
    for index, row in pt.iterrows():
        row.use_yn = row.use_yn.lower()
        row.table_loc = row.table_loc.lower()
    return pt


# In[5]:


def table_exists(table_name, engine):
    """
    테이블 존재 유무확인
    
    Args:
    - table_name : 확인할 테이블 이름
    - engine
    """    
    inspector = sa.inspect(engine)
    return table_name in inspector.get_table_names()


# # 매개변수 받기

# In[18]:


#python파일 실행할 때 매개변수를 받아서
if len(sys.argv) < 2:
    #파라미터 파일 예제 생성
    makeExParameterFile()
    sys.exit()
else:
    inputParam = sys.argv[1]

# isFile
# parameter_table O, parameter_file X : 0 (매개변수가 있고 split 결과 1일 때)
# parameter_table X, parameter_file O : 1 (매개변수가 있고 split 결과 2일 때)
isFile = 0

# 매개변수가 테이블 이름일 때
if len(inputParam.split('.')) == 1:
    tableName = inputParam
    print(f"DB에 {tableName} 이 있는지 확인합니다.")

    # 파라미터 테이블이 저장된 DB 정보
    data = {
        'use_yn': [None],
        'table_loc': ['y'],
        'category': ['0'],
        'db_ip': ['192.168.110.111'],
        'db_id': ['DAJEONG'],
        'db_pw': ['dajeong'],
        'db_port': ['1521'],
        'db_name': ['orcl'],
        'folder_path': [None]
    }
    locDf = pd.DataFrame(data)
    
# 매개변수가 파일 이름일 때
else :
    isFile = 1
    inputFileName = inputParam.split('.')[0]
    inputFileExtension = inputParam.split('.')[1]
    print(f"파일이름 : {inputFileName}, 파일확장자 : {inputFileExtension}")


# In[19]:


if isFile == 0:
    for index, row in locDf.iterrows():
        engine = set_engine(row.CATEGORY, row)

    if table_exists(tableName, engine):
        selquery = sa.text("SELECT * FROM {}".format(tableName))
        pt = pd.read_sql_query(selquery, engine)

        # csv파일이 저장될 DB정보
        useDf = pt[pt['use_yn'] == 'y'].copy()
    else:
        print(f"{tableName}이 존재하지 않습니다. 예제 파일을 생성합니다.")
        makeExParameterFile()
        sys.exit()


# In[28]:


if isFile == 1:
    if inputFileExtension == 'xlsx':
        pt = pd.read_excel("../param/" + inputFileName + "." + inputFileExtension, engine="openpyxl")
    elif inputFileExtension == 'csv':
        pt = pd.read_csv("../param/" + inputFileName + "." + inputFileExtension)

    # 전처리
    pt = pretreatment(pt)
    pt.to_sql(name=inputFileName, if_exists="replace", con=engine, index=False)
        
    # 파라미터테이블이 저장될 DB정보
    locDf = pt[pt['table_loc'] == 'y'].copy()
    
    # csv파일이 저장될 DB정보
    useDf = pt[pt['use_yn'] == 'y'].copy()


# # csv파일 DB에 저장하기

# In[29]:


for index, row in useDf.iterrows():
    engine = set_engine(row.category, row)
    folderPath = row.folder_path.replace(",",".") + "/"
    try:
        if not os.path.exists(folderPath):
            raise FileNotFoundError("폴더가 존재하지 않습니다.")
        fileList = os.listdir(folderPath)
        if not fileList:
            raise FileNotFoundError("폴더가 비어있습니다.")
    except FileNotFoundError as e:
        print(e)
        sys.exit()
    except Exception as e:
        print("예상치 못한 오류가 발생했습니다:", e)
        sys.exit()

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

