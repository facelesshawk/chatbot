# how to make chatbot in python code by sentdex

import sqlite3
import json
from datetime import datetime

timeframe='2015-5'
sql_transaction=[]
connection=sqlite3.connect('{}.db'.format(timeframe))
c=connection.cursor()

def create_table():
	#we have created the table with name parent_reply
	c.execute("""create table if table does not Exists parent_reply(parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE,parent TEXT,comment TEXT,subreddit TEXT,
	unix INT,score INT)""")
	
def format_data(data):
	data=data.replace("\n","newlinechar").replace("\r","newlinechar").replace('"',"'")
	return data

# here we are creating the stuff that data is acceptable or not
def acceptable(data):
	if len(data.split(" "))>50 or len(data)<1:
		return False
	elif len(data)>1000:
		return False
	elif data=='[deleted]' or data=='[removed]':
		return False
	else:
		return True
	
def find_parent(pid):
	try:
		sql="SELECT comment FROM parent_reply WHERE comment_id ='{}' LIMIT=1".format(pid)
		c.execute(sql)
		result=c.fetchone()
		if result!=NONE:
			#we have selected 0 because we only want to select one comment at a time.
			return result[0]
		else:
			return False
	except Exception as e:
		#print("find_parent",e)
		return False
		
def find_existing_score(pid):
	try:
		sql="SELECT score FROM parent_reply WHERE parent_id ='{}' LIMIT=1".format(pid)
		c.execute(sql)
		result=c.fetchone()
		if result!=NONE:
			#we have selected 0 because we only want to select one comment at a time.
			return result[0]
		else:
			return False
	except Exception as e:
		#print("find_parent",e)
		return False
		
#here we are going to make out transaction_bldr fn
def transaction_bldr(sql):
	global sql_transaction
	sql_transaction.append(sql)
	if len(sql_transaction)>1000:
		c.execute('BEGIN TRANSACTION')
		for s in sql_transaction:
			try:
				c.execute(s)
			except:
				pass
		connection.commit()
		sql.transaction=[]
		
		
def sql_insert_replace_comment(commentid,parentid,parent,comment,subreddit,time,score):
	try:
		sql="""UPDATE parent_reply SET parent_id=?, comment_id=?,parent=?,comment=?,subreddit=?,unix=?,score=? WHERE parent_id=?;""".format(parentid)
		transaction_bldr(sql) # transaction builder
	except Exception as e:
		print("s-UPDATE insertion",str(e))

def sql_insert_has_parent(commentid,parentid,parent,comment,subreddit,time,score):
	try:
		sql="""INSERT INTO parent_reply(comment_id,parent_id,parent,comment,subreddit,unix,score)"""
		transaction_bldr(sql) # transaction builder
	except Exception as e:
		print("s-PARENT insertion",str(e))
		
def sql_insert_no_parent(commentid,parentid,parent,comment,subreddit,time,score):
	try:
		sql="""INSERT INTO parent_reply(comment_id,parent_id,parent,comment,subreddit,unix,score)"""
		transaction_bldr(sql) # transaction builder
	except Exception as e:
		print("s-NOPARENT insertion",str(e))		
		
if __name__="__main__":
	create_table()
	#as we are iterating through this files row_counter will tell how many files we have iterated
	row_counter =0
	#paired_rows tell us that how many parent and child are pair.(because some of these comments may be unreply)
	paired_rows=0
	# now we will open one of the file 
	with open("pathof the file/data/{}/RC_{}".format(timeframe.split('-')[0],timeframe),buffering=10000) as f:
		for row in f:
			#print(row)
			row_counter+=1
			row=json.loads(row)
			parent_id=row['parent_id']
			#here format_data is a fn used for formatting the data into better way.
			body=format_data(row['body'])
			created_utc=row['created_utc']
			score=row['score']
			subreddit=row['subreddit']
			comment_id=row['name']
			#there might be some instances when we need to find the parents.So,
			parent_data=find_parent(parent_id)
			
			#there are many uninterested comments on the reddit so we want to eliminate those so we will use score to figure out those.
			#score >=2 means somebody have commented even a single emoji
			if score >=2:
				if acceptable(body):
					#we also want to check out that the if the same parent id have better score.
					#if existing score is greater than current then we donot care else we will replace with new reply, new database					
					existing_comment_score=find_existing_score(parent_id)
					if existing_comment_score:
						if score > existing_comment_score:
						# we are going to update the data
							sql_insert_replace_comment(comment_id,parent_id,parent_data,body,subreddit,created_utc,score)
					
					else:
						if parent_data:
							##
							sql_insert_has_parent(comment_id,parent_id,parent_data,body,subreddit,created_utc,score)
							paired_rows+=1
						else:
							##if there will be no parent then reddit thread is the parent id in reddit
							#and there is also one more reason to store this because this data may be the parent of other data
							sql_insert_no_parent(comment_id,parent_id,body,subreddit,created_utc,score)
							
		if row_counter%100000==0:
			print("total rows:{}, paired rows: {},time:{}".format(rows_counter,paired_rows,str(datetime.now())))
	
	