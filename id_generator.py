import json
import sys
import logging
import pymysql
import os
import time
import datetime
import asyncio
import boto3
from botocore.exceptions import ClientError

# getting credientials from variable.tf file
REGION = os.environ['AWS_REGION']
rds_host = os.environ['host']
name = os.environ['username']
password = os.environ['password']
db_name = os.environ['database']
myport = 3306   
init_database = os.environ['init_database']

logger = logging.getLogger()
logger.setLevel(logging.INFO)
    
# This try block makes a connection with RDS
try:
    conn = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, port=myport)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

async def id_Generate(AppName, AppType, user):
    
    """
    This method generates Id as a sequence number for every application and stores that Id along with application name, sequence type, current date, 
    time and user information in the SequenceIds table.  
    """

    try:
        now = datetime.datetime.utcnow()
        mytime = time.localtime()
        with conn.cursor() as cur:

            date = str(now.strftime('%Y-%m-%d'))
            # time = str(now.strftime('%H:%M:%S'))
            current_time = time.strftime("%H:%M:%S", mytime)

            table_name = "SequenceIds"
            cur.execute(f"insert into {table_name} (AppName, Type, Date, Time, User) values('{AppName}', '{AppType}', '{date}', '{current_time}', '{user}')")
                            
            latestId = cur.lastrowid
            print(latestId)
            
        conn.commit()
        return {"current":str(latestId)}
    except Exception as e:
        print("Error was returned by id_Generate method: "+ str(e))

async def saveData(appName, type, user):
    """
    This method is used to check the Id which was generated most recently by the Id_Generate method. If the Id is out of range then 
    this method deletes that Id from the SequenceIds table. If within range it updates the Id in the SequenceIds table to keep track 
    of the latest Id generated and to avoid exceeding Id range values.
    """
    try:
        
        with conn.cursor() as cur:

            Id = await id_Generate(appName, type, user)
            latestId = Id['current']

            range_table = "SequenceRange"
            seqId_table = "SequenceIds"

            result = cur.execute(f"SELECT * FROM {range_table} WHERE Status = 'True' ")
            result = cur.fetchall()

            
            for row in result:
                rows = json.dumps(row)
                res = json.loads(rows)

                startId = res[1]
                endId = res[2]

            if(int(latestId) >=int(startId) and int(latestId) <= int(endId)):

                # with conn.cursor() as cur:
                                        
                cur.execute(f"UPDATE {seqId_table} SET Id = {str(latestId)} WHERE Id= '{latestId}'")
                                            
                # conn.commit()
                return {
                    'number' : latestId,
                    'message': f"{type} number for {appName} returned successfully."
                }

            else:
                print("Please define a new range.")
                # with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {seqId_table} WHERE ID = {str(latestId)}")
                # conn.commit()
                
                return {
                    'number' : 200,
                    'message': f"The currently active range of Ids is exhausted. Please assign new range. " 
                }
            
        conn.commit()

    except Exception as e:
        print("Error was returned by saveData method: "+ str(e))

async def assignRange(startRange, EndRange):
    """
    This method is used to assigns new sequence range in the SequenceRange table and set status false to the previous range.
    """
    try:
        # previous_range = [0, 0]  # Example previous range
        new_range = [startRange, EndRange]     # Example new range to be assigned

        with conn.cursor() as cur:
            range_table = "SequenceRange"
            sequenceId_tables = "SequenceIds"
                
            result = cur.execute(f"SELECT * FROM {range_table} WHERE Status = 'True' ")
            result = cur.fetchall()

            for row in result:
                rows = json.dumps(row)
                res = json.loads(rows)

                startId = res[1]
                endId = res[2]
                previous_range = [startId, endId]

            if int(new_range[0]) > int(previous_range[1]):
                # New range starts after the end of the previous range
                assigned_range = new_range
                cur.execute(f"UPDATE {range_table} SET Status = 'False' WHERE Status = 'True' ")
                cur.execute(f"INSERT INTO {range_table} (StartRange, EndRange, Status ) values('{new_range[0]}', '{new_range[1]}', 'True')")
                cur.execute(f"ALTER TABLE {sequenceId_tables} AUTO_INCREMENT = {new_range[0]};")
                print("New range assigned successfully which is :", assigned_range)
                return{
                    'number': 200,
                    'message': f"New range assigned successfully which is : {assigned_range}"
                }
            elif int(new_range[1]) < int(previous_range[0]):
                # assigned_range = [int(previous_range[1]) + 1, int(new_range[1]) + (int(previous_range[1]) - int(new_range[0]) + 1)]
                start = int(previous_range[1]) + 1
                end = int(new_range[1]) + (int(previous_range[1]) - int(new_range[0]) + 1)
                return{
                    'number': 200,
                    'message': f"This range is smaller than current range, You can not assign this range, You should assign range from {start} to {end}"
                }
            else:
                # New range overlaps with the previous range, so adjust it
                assigned_range = [int(previous_range[1]) + 1, int(new_range[1]) + (int(previous_range[1]) - int(new_range[0]) + 1)]
                print(f"You should assign range from {int(previous_range[1]) + 1} to {int(new_range[1]) + (int(previous_range[1]) - int(new_range[0]) + 1)}")
                return{
                    'number': 200,
                    'message': f"You should assign range from {int(previous_range[1]) + 1} to {int(new_range[1]) + (int(previous_range[1]) - int(new_range[0]) + 1)}"
                }
            
        conn.commit()

    except Exception as e:
        print("Error was returned by assignRange method: "+ str(e))

async def getData():

    ''' This method gets only latest Id data for all applications from SequenceIds table and returns . '''
    try:
            
        with conn.cursor() as cur:
            table_name = "SequenceIds"
        
            result = cur.execute(f"SELECT DISTINCT AppName, Type FROM {table_name} ")
            result = cur.fetchall()

            body = []
            for row in result:
                rows = json.dumps(row)
                res = json.loads(rows)

                appName = res[0]
                appType = res[1]
                previous_range = [appName, appType]
                # print(f"{res} \n")

                # previous_ranges = previous_ranges.__add__([previous_range])
                get_record = cur.execute(f"SELECT * FROM {table_name} WHERE AppName = '{previous_range[0]}' AND Type = '{previous_range[1]}' ORDER BY Id desc LIMIT "+str(1))
                get_record = cur.fetchall()
                body = body.__add__([get_record[0]])

            # body = cur.execute(f"DROP TABLE SequenceRange")

        conn.commit()
        return {
            'number': 200,
            'message': body 
        }

    except Exception as e:
        print("Error was returned by getData method: "+ str(e))

async def getDataParams(AppName,Type):
    ''' This method gets only latest Id data for a specific application from SequenceIds table and display that data in the browser. '''
    try:
        with conn.cursor() as cur:
            table_name = "SequenceIds"
            
            result = cur.execute(f"SELECT DISTINCT AppName, Type FROM {table_name} ")
            result = cur.fetchall()

            
            AppNames = []
            for row in result:
                rows = json.dumps(row)
                res = json.loads(rows)

                appName = res[0]
                appType = res[1]
                previous_range = [appName, appType]
                
                print(previous_range)
                AppNames = AppNames.__add__([previous_range])
                
                
            AppMatch = False
            for range in AppNames:

                if(AppName == range[0] and Type == range[1]):
                    AppMatch = True
                    break
                
            if AppMatch:
                
                qry = f"SELECT * FROM {table_name} WHERE AppName = '{AppName}' AND Type = '{Type}' ORDER BY Id desc LIMIT "+str(1)
                cur.execute(qry)
                   
                rec = cur.fetchall()
                    
                body = rec
                print(body)
                    
            else:
                return{
                    'number': 200,
                    'message': json.dumps("Application/Type not found")
                }

        conn.commit()

        return {
            'number': 200,
            'message': body 
        }

    except Exception as e:
        print("Error was returned by getDataParams method: "+ str(e))

async def getDataById(Id,AppName,Type):
    ''' This method gets data for a specific Id from SequenceIds table and display that data in the browser. '''
    try:
            
        with conn.cursor() as cur:
            table_name = "SequenceIds"
            
            result = cur.execute(f"SELECT DISTINCT AppName, Type FROM {table_name} ")
            result = cur.fetchall()

            AppNames = []
            for row in result:
                rows = json.dumps(row)
                res = json.loads(rows)

                appName = res[0]
                appType = res[1]
                previous_range = [appName, appType]
                # print(f"{res} \n")
                AppNames = AppNames.__add__([previous_range])

            AppMatch = False
            for range in AppNames:

                if(AppName == range[0] and Type == range[1]):
                    AppMatch = True
                    break

            if AppMatch:
                qry = f"SELECT * FROM {table_name} WHERE AppName = '{AppName}' AND Type = '{Type}' AND Id = '{Id}' "
                rows_count = cur.execute(qry)
                if rows_count >0:
                    rec = cur.fetchall()
                else:
                    return{
                        'number': 200,
                        'message': "Id not found"
                    }
                body = rec
                print(body)
            else:
                return{
                    'number': 200,
                    'message': "Application/Type not found"
                }

        conn.commit()
        if body == "":
            return{
                'number': 200,
                'message': "Id not found"
            }
        else:
            return {
                'number': 200,
                'message': body 
            }

    except Exception as e:
        print("Error was returned by getDataById method: "+ str(e))

# Handle API Request
async def invoke_lambda(event): 

    ''' This method is checking if tables exists in RDS or not if tables exists in RDS it simply gets all data from 
    the table otherwise it will call another lambda function that will create a table in RDS and then insert a record in that table or get 
    the data from RDS and display in browser. '''
    
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    
    getLambdaMethod = event['requestContext']['http']
    
    try:
        with conn.cursor() as cur:
            table_name = 'SequenceRange'
            _SQL = """SHOW TABLES"""
            cur.execute(_SQL)   # check the table in RDS
            results = cur.fetchall()
    
            results_list = [item[0] for item in results] # Conversion to list of str
                    
            if table_name in results_list: # if table already exists in RDS this block is executed and inserts data in database
    
                if getLambdaMethod['method'] == POST: # check API Gateway Request
                    decodedBody = json.loads(event['body']) # get body response
                    
                    result = await saveData(decodedBody["AppName"], decodedBody["Type"], decodedBody["User"])
                    final_result = result
            
                elif getLambdaMethod['method'] == PUT: # check API Gateway Request
                    decodedBody = json.loads(event['body']) # get body response
                        
                    StartRange = decodedBody["StartRange"]
                    EndRange = decodedBody["EndRange"]

                    if(int(StartRange) < int(EndRange)):

                        if "UserRole" in decodedBody:

                            UserRole = decodedBody["UserRole"]

                            if(UserRole == "Admin"):
                            
                                result = await assignRange(StartRange, EndRange)
                                final_result = result
                            else:
                                final_result = "message: You have no permissions to assign new range"
                        else:
                                final_result = "message: You have no permissions to assign new range"
                    else:
                        final_result = "message: Please assign a new range in assending order"
                    
                # Handle API GET Request
                elif getLambdaMethod['method'] == GET:
                    print(event)
                    
                    if 'queryStringParameters' in event:
                        
                        decodedBody = event['queryStringParameters']
                        
                        if "Id" in decodedBody:
                            print(decodedBody["Id"], decodedBody["AppName"], decodedBody["Type"])
                            result = await getDataById(decodedBody["Id"], decodedBody["AppName"], decodedBody["Type"])    
                        else:
                            print(decodedBody["AppName"], decodedBody["Type"])
                            result = await getDataParams(decodedBody["AppName"], decodedBody["Type"])
                    else:
                        result= await getData()

                    final_result = result
    
            else:

                if getLambdaMethod['method'] == POST: # check API Gateway Request
                    decodedBody = json.loads(event['body']) # get body response
                    
                    result = await saveData(decodedBody["AppName"], decodedBody["Type"], decodedBody["User"])
                    final_result = result
            
                elif getLambdaMethod['method'] == PUT: # check API Gateway Request
                    decodedBody = json.loads(event['body']) # get body response
                    
                    StartRange = decodedBody["StartRange"]
                    EndRange = decodedBody["EndValue"]
                    
                    if(int(StartRange) < int(EndRange)):

                        if "UserRole" in decodedBody:
                            UserRole = decodedBody["UserRole"]

                            if(UserRole == "Admin"):
                            
                                result = await assignRange(StartRange, EndRange)
                                final_result = result
                            else:
                                final_result = "message: You have no permissions to assign new range"
                        else:
                                final_result = "message: You have no permissions to assign new range"
                    else:
                        final_result = "message: Please assign a new range in assending order"

                # Handle API GET Request
                elif getLambdaMethod['method'] == GET:

                    invokeLam = boto3.client("lambda",region_name=REGION)
                    payload = {"Type":"Account/Policy", "AppName":"Arden/Surety/GuideWire"}
                        
                    invokeLam.invoke(FunctionName=init_database, InvocationType="RequestResponse", Payload=json.dumps(payload))
                    
                    if 'queryStringParameters' in event:   
                        decodedBody = event['queryStringParameters']
                        if "Id" in decodedBody:
                            print(decodedBody["Id"], decodedBody["AppName"], decodedBody["Type"])
                            result = await getDataById(decodedBody["Id"],decodedBody["AppName"], decodedBody["Type"])    
                        else:
                            print(decodedBody["AppName"], decodedBody["Type"])
                            result = await getDataParams(decodedBody["AppName"], decodedBody["Type"])
                    else:
                        result= await getData()
                        
                    final_result = result
        conn.commit()

        # print(str(final_result))
        return final_result
        
    except Exception as e:
        print("Error was returned by invoke_lambda function: "+ str(e))
    
def lambda_handler(event, context):
    
    try:
        start_time = time.perf_counter()
        
        res = asyncio.run(invoke_lambda(event))
                    
        stop_time = time.perf_counter()
        elapse_time = stop_time - start_time
        print(f"request time is : {elapse_time*1000} milliseconds")
    
        return res 
    except Exception as e:
        return{"Error":"Request Error "}