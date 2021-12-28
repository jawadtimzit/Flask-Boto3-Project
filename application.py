import requests
import boto3
import botocore
from flask import Flask, render_template, request, redirect, url_for, flash
from boto3.dynamodb.conditions import Key
import botocore.vendored.requests.packages.urllib3 as urllib3
import os
from urllib3 import PoolManager
import time
import sys
application = Flask(__name__, template_folder='templates')
application.secret_key = 'secret_key-here'
Load_List=[]
empty_string=""
heading=[]

@application.route("/returntomain",methods=["GET","POST"])
def ReturnMain():
    return render_template("base.html")


@application.route("/",methods=["GET","POST"])
def default():
    if request.method=="POST":
        if request.form['Clear']=='Clear':
            print('Hello Clear!', file=sys.stderr)
            clear()
            message="Object is Cleared and Database Table Deleted Successfully"
            return render_template("base.html" , message=message)
    return render_template("base.html")
    
def clear():
    try:
        dynamodb = boto3.resource('dynamodb',
                            region_name='us-west-1',
                            aws_access_key_id="AWS_Key_here",
                            aws_secret_access_key="Secrect-Access-key-here")
                            #aws_session_token=keys.AWS_SESSION_TOKEN)
        table = dynamodb.Table('users') 
        table.delete()    
        Load_List.clear()
        #flash("Successfully clear and Delete Dynamo Table")
    except Exception as e:
        print(e)
@application.route("/Query",methods=["GET","POST"])
def Query():
    if request.method =="POST":
        if request.form['Query']=='Query':
            print('Hello Query!', file=sys.stderr)
            first=request.form['FirstName']
            last=request.form['LastName']
            if not first and not last:
                message="Both Feild Must be Filled"
                return render_template("base.html",message=message) 
            print('firstName : ' + first + '  lastName : ' + last)
            response=query(first,last)
            if response==None:
                message="Names Does Not exists in the Database"
                return render_template("base.html",message=message)
            else:
                if response['Count']==0:
                    message="Names Does Not exists in the Database"
                    return render_template("base.html",message=message)
                info_detail=response['Items']
                return render_template("page1.html", info_detail=info_detail)
            return render_template("page1.html")
        
    return render_template("base.html")

def query(first, last):
    dynamodb = boto3.resource('dynamodb',
                        region_name='us-west-1',
                        aws_access_key_id="AWS_Key_here",
                        aws_secret_access_key="Secrect-Access-key-here")
    table = dynamodb.Table('users')
    print('firstName : ' + first + '  lastName : ' + last)
    if not first or not last:
        if first:
            try:
                # creating a dynamoDb query from based on attributes
                queryOutput = table.query(
                    KeyConditionExpression=Key('FirstName').eq(first)
                )
                return queryOutput
            except Exception as e:
                print(e)
        elif last:

            try:
                # creating a dynamoDb query output object
                print("Hello Last")
                queryOutput = table.query(IndexName='LastName-index',KeyConditionExpression=Key('LastName').eq(last))
                return queryOutput
            except Exception as e:
                print(e)
    else:

        try:
            # query based on the table key
            queryOutput = table.query(KeyConditionExpression=Key('FirstName').eq(first) & Key('LastName').eq(last))
            return queryOutput
        except Exception as exception:
            print(exception)

@application.route("/load", methods=["GET","POST"])
def Load():
    if request.method =="POST":
        print('Hello Load!', file=sys.stderr)
        create_db()
        text=awsCall()
        getData(text)
        print(Load_List, file=sys.stderr)
        for row in Load_List:
            for keyvalue in row:
                if len(heading)==0:
                   heading.append(keyvalue)
                else:
                   if not keyvalue in heading:
                       heading.append(keyvalue)
        return render_template("load.html",heading=heading,Load_List=Load_List,empty_string=empty_string)
def awsCall():
    page_source = requests.get('http://s3-us-west-2.amazonaws.com/css490/input.txt')
    text=page_source.text.split('\n')
    return text
def getData(text):
    for i in range(len(text)):
        addString=text[i].split(' ')
        if len(addString)>2:
            x={}
            x['FirstName']=addString[0]
            x['LastName']=addString[1]
            for attr in addString[2:]:
                if '\r' in attr:
                    attr=attr.replace('\r','')
                if attr=='' or attr==' ':
                    continue
                attr_val=attr.split('=')
                x[attr_val[0]]=attr_val[1]
            Load_List.append(x)
            print(x)
            Update_DB(x)

def Update_DB(x):
    try:
        dynamodb = boto3.resource('dynamodb',
                            region_name='us-west-1',
                            aws_access_key_id="AWS_Key_here",
                            aws_secret_access_key="Secrect-Access-key-here")
                            #aws_session_token=keys.AWS_SESSION_TOKEN)
        table = dynamodb.Table('users')
        table.put_item(
           Item=x
        )
    except Exception as e:
        print(e)

def create_db():   
    dynamodb = boto3.resource('dynamodb',
                        region_name='us-west-1',
                        aws_access_key_id="AWS_Key_here",
                        aws_secret_access_key="Secrect-Access-key-here")
    table_name = 'users'
    dynamodb_client = boto3.client('dynamodb',region_name='us-west-1',aws_access_key_id="AWS_Key_here",aws_secret_access_key="Secrect-Access-key-here")
    existing_tables = dynamodb_client.list_tables()['TableNames']
    if table_name not in existing_tables:
        # Create the DynamoDB table.
        table = dynamodb.create_table(
            TableName='users',
             AttributeDefinitions=[
                {
                    'AttributeName': 'FirstName',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'LastName',
                    'AttributeType': 'S'
                }
            ],
            KeySchema=[
                {
                    'AttributeName': 'FirstName',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'LastName',
                    'KeyType': 'RANGE'
                }
            ],
           
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        
        # Wait until the table exists.
        table.meta.client.get_waiter('table_exists').wait(TableName='users')

# Print out some data about the table.
#print(table.item_count)


if __name__ == '__main__':
    application.run(debug=True)