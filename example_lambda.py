import json
import sys
import logging
import pymysql
import os
import time
import datetime
import asyncio

# getting crediential from variable.tf file
REGION = os.environ['AWS_REGION']
rds_host = os.environ['host']
name = os.environ['username']
password = os.environ['password']
db_name = os.environ['database']
myport = 3306   

logger = logging.getLogger()
logger.setLevel(logging.INFO)
    
# This try block is Making a connection with RDS
try:
    conn = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, port=myport)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

async def idGenerate(tableName, start, AppName, Type, user):

    '''This method generates Id for applications like GuideWire, Surety, Arden and APS using Autoincrement and saves that id in a
    separate table for each application. This method takes two parameters, first is tableName and the second is start. tableName provides
    table name in the database and start parameter provides the starting number for autoincrementing value in the database. This method will check if table
    exists in the database or not if it already exists then it simply inserts a record in the table otherwise it will first create a table in the database 
    then insert a record in it. '''
    try:
        now = datetime.datetime.utcnow()
        mytime = time.localtime()
        latestId = ""
        with conn.cursor() as cur:
            table_name = tableName
            _SQL = """SHOW TABLES"""
            cur.execute(_SQL) # check if table exists in RDS 
            results = cur.fetchall()
                    
            results_list = [item[0] for item in results] # Convert tables to array list

            date = str(now.strftime('%Y-%m-%d'))
            # time = str(now.strftime('%H:%M:%S'))
            current_time = time.strftime("%H:%M:%S", mytime)

            if((AppName=="Arden" and Type == "Account") or (AppName=="Surety" and Type == "Account") or (AppName=="GuideWire" and Type == "Account") or (AppName=="GuideWire" and Type == "Policy")):

                if table_name in results_list: # if table exists in RDS, this block is executed and inserts values into in database

                    
                    cur.execute(f"insert into {table_name} (AppName, Type, Date, Time, User) values('{AppName}', '{Type}', '{date}', '{current_time}', '{user}')")
                        
                    latestId = cur.lastrowid
                            
                else: # if table does not exist in RDS, then this block creates a new table in RDS and then inserts data in database
                    cur.execute(f'CREATE TABLE {table_name} (Id INT UNSIGNED NOT NULL AUTO_INCREMENT, AppName varchar(255), Type varchar(255), Date varchar(255), Time varchar(255), User varchar(255), PRIMARY KEY (Id))AUTO_INCREMENT = {start};')
                            
                    cur.execute(f"insert into {table_name} (AppName, Type, Date, Time, User) values('{AppName}', '{Type}', '{date}', '{current_time}', '{user}')")
                        
                    latestId = cur.lastrowid

        conn.commit()
        return {"current":str(latestId)}
            
        
    except Exception as e:
        print("ERROR: Unexpected error.."+ str(e))   

async def updateReservedIds(latestId, startId, endId, tbl_name_for_sequence, appName, type, ):

    """ This method is used to update reserve id in database. It takes 6 parameters namely, latestId, startId, endId, tbl_name_for_sequence, 
    appName and type. Here startId is the starting point of reserved id and endId is the ending point of  reserved id and latestId is 
    the currentId which will be updated in sequence table. Here this method is checking if latestId is between startId and endId, if so, 
    it will update currentvalue in sequence table otherwise it will ignore latestId."""
    try:
        
        if(int(latestId) >=int(startId) and int(latestId) <= int(endId)):

            with conn.cursor() as cur:
                                    
                cur.execute(f"UPDATE {tbl_name_for_sequence} SET CurrentValue = {str(latestId)} WHERE AppName= '{appName}' AND Type= '{type}'")
                                        
            conn.commit()
            return {
                'body': json.dumps(f"{type} number for {appName} is {latestId}.") 
            }
        else:
            tbl_name_for_idgenerate = appName+type+"IDS"
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {tbl_name_for_idgenerate} WHERE ID = {str(latestId)}")
            conn.commit()
            
            return {
                'body': json.dumps(f"{type} number is blocked for {appName}.") 
            }
       
    except Exception as e:
        print("ERROR: Unexpected error.."+ str(e))

async def updatePolicyIds(tbl_name_for_sequence, appName, type, latestId):

    ''' This method is used to update Policy id in sequence table '''
    try:
        startSurety = ""
        endSurety = ""
        startArden = ""
        endArden =""
        prevId=latestId
                        
        with conn.cursor() as cur:
                                    
            surety_record = cur.execute(f"SELECT * FROM {tbl_name_for_sequence} WHERE AppName= 'Surety' AND Type= 'Account'")
            surety_record = cur.fetchall()
                            
            for row1 in surety_record:
                startSurety = row1[2]
                endSurety = row1[3]
                print("startSurety", row1[2])
                print("endSurety", row1[3])
                
            arden_record = cur.execute(f"SELECT * FROM {tbl_name_for_sequence} WHERE AppName= 'Arden' AND Type= 'Account'")
            arden_record = cur.fetchall()
                            
            for row2 in arden_record:
                startArden = row2[2]
                endArden = row2[3]
                print("startArden", row2[2])
                print("endArden", row2[3])
               
                                        
        conn.commit()

        ''' This method checks whether the latestId falls between the startId and endId values for Arden and Surity applications. If so, ignore the latestId and skip to the next number after endId and return. 
        otherwise (latestId not between startId and endId), simply increment by 1 and return the resulting number'''

        if int(startArden) < int(startSurety) and int(endArden) + 1 == int(startSurety):
    
            if int(latestId) == int(startArden):
                latestId = int(endSurety) + 1

        elif int(startSurety) < int(startArden) and int(endSurety) + 1 == int(startArden):
            if int(latestId) == int(startSurety):
                latestId = int(endArden) + 1
                           
        else:
            if int(startSurety) < int(startArden):
                if int(latestId) == int(startSurety):
                    latestId = int(endSurety) + 1
                                                        
                elif int(latestId) == int(startArden):
                    latestId = int(endArden) + 1
                                 
            elif int(startArden) < int(startSurety):
                if int(latestId) == int(startSurety):
                    latestId = int(endSurety) + 1
                                    
                elif int(latestId) == int(startArden):
                    latestId = int(endArden) + 1
                    
        with conn.cursor() as cur:
                                
            cur.execute(f"UPDATE {tbl_name_for_sequence} SET CurrentValue = {str(latestId)} WHERE AppName= '{appName}' AND Type= '{type}'")
            cur.execute(f"UPDATE {appName+type+'IDS'} SET Id = {str(latestId)} WHERE Id= '{prevId}'")

        conn.commit() 

        return {
            'body': json.dumps(f"{type} number for {appName} is {latestId}.") 
        }             

    except Exception as e:
        print("ERROR: Unexpected error.."+ str(e))

async def updateSequence(appName, type, user):

    ''' This method gets latestId from idGenerate method and provides that id to updateReservedIds method and 
    updatePolicyIds method according to condition '''
    try:
        tbl_name_for_sequence = "sequence"
        tbl_name_for_idgenerate = appName+type+"IDS"
                    
        startId = ""
        endId = ""
                    
        with conn.cursor() as cur: # get seed data from database
                                
            records = cur.execute(f"SELECT * FROM {tbl_name_for_sequence} WHERE AppName= '{appName}' AND Type= '{type}'")
            records = cur.fetchall()
                        
            for row in records:
                startId = row[2] # get startId from RDS
                endId = row[3]
                
        conn.commit()
                    
        val = await idGenerate(tbl_name_for_idgenerate, startId, appName, type, user)  # method for generating Id
        latestId = val['current'] # latest id from RDS

        if(appName=="GuideWire" and type=="Account"):
            with conn.cursor() as cur: # Update data in RDS
                cur.execute(f"UPDATE {tbl_name_for_sequence} SET CurrentValue = {str(latestId)} WHERE AppName= '{appName}' AND Type= '{type}'")
            conn.commit()
            
            return json.dumps(f"{type} number for {appName} is {latestId}.")

        elif(appName=="GuideWire" and type=="Policy"):

            reserveId = await updatePolicyIds(tbl_name_for_sequence, appName, type, latestId)
            return reserveId['body']

        elif((appName=="Surety" and type == "Account") or (appName=="Arden" and type == "Account")):

            reserveId = await updateReservedIds(latestId, startId, endId, tbl_name_for_sequence, appName, type)
            
            return str(reserveId['body'])

        elif((appName=="Surety" and type == "Policy") or (appName=="Arden" and type == "Policy")):

            return json.dumps(f"{type} is blocked for {appName}.")

        elif((appName=="Surety" and type == "Quote") or (appName=="Arden" and type == "Quote") or (appName=="GuideWire" and type == "Quote")):
            
            return json.dumps(f"{type} is blocked for {appName}.")

        else:
            
            return json.dumps(f"message: Cannot generate any number for {appName}")
    
    except Exception as e:
        print("ERROR: Unexpected error.."+ str(e))

async def getData():

    ''' This method is checking if sequence table exists in RDS or not if sequence table exists in RDS it simply gets all data from 
    the table otherwise it will create a table with the name of sequence in RDS and then insert a record in that table and then get 
    all data from it and display in browser '''
    try:
            
        with conn.cursor() as cur:
            table_name = 'sequence'
            _SQL = """SHOW TABLES"""
            cur.execute(_SQL)   # check the table in RDS
            results = cur.fetchall()

            results_list = [item[0] for item in results] # Conversion to list of str
                
            if table_name in results_list: # if table already exists in RDS this block is executed and inserts data in database

                qry = f"select * from {table_name}"
                cur.execute(qry)
                rec = cur.fetchall()
                body = rec

            else:
                ###if table does not exist in RDS this block creates a new table in RDS and then inserts data in database
                cur.execute(f'CREATE TABLE {table_name} (Id INT UNSIGNED NOT NULL AUTO_INCREMENT,'+ 
                'AppName varchar(50), StartId INT, EndId INT, Type varchar(50), CurrentValue INT, PRIMARY KEY (Id));')
                                
                cur.execute(f'insert into {table_name} (AppName, StartId, EndId, Type, CurrentValue) '+
                'values("GuideWire", "30000000", "Null", "Account", "Null")')
                cur.execute(f'insert into {table_name} (AppName, StartId, EndId, Type, CurrentValue) '+
                'values("GuideWire", "4000000", "Null", "Policy", "Null")')
                cur.execute(f'insert into {table_name} (AppName, StartId, EndId, Type, CurrentValue) '+
                'values("GuideWire", "Null", "Null", "Quote", "Null")')
        
                cur.execute(f'insert into {table_name} (AppName, StartId, EndId, Type, CurrentValue) '+
                'values("Arden", "8000000", "8050000", "Account", "Null")')
                cur.execute(f'insert into {table_name} (AppName, StartId, EndId, Type, CurrentValue) '+
                'values("Arden", "Null", "Null", "Policy", "Null")')
                cur.execute(f'insert into {table_name} (AppName, StartId, EndId, Type, CurrentValue) '+
                'values("Arden", "Null", "Null", "Quote", "Null")')
        
                cur.execute(f'insert into {table_name} (AppName, StartId, EndId, Type, CurrentValue) '+
                'values("Surety", "8050001", "8060000", "Account", "Null")')
                cur.execute(f'insert into {table_name} (AppName, StartId, EndId, Type, CurrentValue) '+
                'values("Surety", "Null", "Null", "Policy", "Null")')
                cur.execute(f'insert into {table_name} (AppName, StartId, EndId, Type, CurrentValue) '+
                'values("Surety", "Null", "Null", "Quote", "Null")')
        
                cur.execute(f'insert into {table_name} (AppName, StartId, EndId, Type, CurrentValue) '+
                'values("APS", "Null", "Null", "Account", "Null")')
                cur.execute(f'insert into {table_name} (AppName, StartId, EndId, Type, CurrentValue) '+
                'values("APS", "Null", "Null", "Policy", "Null")')
                cur.execute(f'insert into {table_name} (AppName, StartId, EndId, Type, CurrentValue) '+
                'values("APS", "Null", "Null", "Quote", "Null")')
    
                qry = f"select * from {table_name}"
                cur.execute(qry)
                rec = cur.fetchall()
                body = rec
                    
        conn.commit()
        return {
            'statusCode': 200,
            'body': json.dumps(body) 
        }

    except Exception as e:
        print("ERROR: "+ str(e))


async def getDataParams(AppName,Type):

    ''' This method is checking if sequence table exists in RDS or not if sequence table exists in RDS it simply gets all data from 
    the table otherwise it will create a table with the name of sequence in RDS and then insert a record in that table and then get 
    all data from it and display in browser '''
    try:
            
        with conn.cursor() as cur:
            table_name = AppName+Type+'IDS'
            print(table_name)
            _SQL = """SHOW TABLES"""
            cur.execute(_SQL)   # check the table in RDS
            results = cur.fetchall()
            print(results)
            results_list = [item[0] for item in results] # Conversion to list of str
            
            if table_name in results_list: # if table already exists in RDS this block is executed and inserts data in database

                qry = f"select * from {table_name} ORDER BY Id desc LIMIT "+str(1)
                print (qry)
                cur.execute(qry)
                rec = cur.fetchall()
                body = rec


        conn.commit()
        return {
            'statusCode': 200,
            'body': json.dumps(body) 
        }

    except Exception as e:
        print("ERROR: "+ str(e))

# Handle API Request
def invoke_lambda(event): 

    ''' This method is used to send body response to the updateSequence method '''
    GET = "GET"
    POST = "POST"
    
    getLambdaMethod = event['requestContext']['http']

    if getLambdaMethod['method'] == POST: # check API Gateway Request
        decodedBody = json.loads(event['body']) # get body response
        result = updateSequence(decodedBody["AppName"], decodedBody["Type"], decodedBody["User"])
        final_result = result

    # Handle API GET Request
    elif getLambdaMethod['method'] == GET :
        
        print (event) 
        if 'body' in event:   
            decodedBody = json.loads(event['body'])
            print(decodedBody["AppName"], decodedBody["Type"])
            result = getDataParams(decodedBody["AppName"], decodedBody["Type"])
        else:
            result=getData()
        final_result = result
        
    return final_result
    
def lambda_handler(event, context):
    
    start_time = time.perf_counter()
    
    res = asyncio.run(invoke_lambda(event))
    
    stop_time = time.perf_counter()
    elapse_time = stop_time - start_time
    print(f"request time is : {elapse_time*1000} milliseconds")
    
    return res  