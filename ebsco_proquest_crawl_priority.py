import MySQLdb as mdb
import pandas as pd
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
import requests
import time

def mysql_con(host="se-db.prod-se.aws.learningace.com", user="root" , passwd="aceuser123", db="tmp",auto_commit=True):
	print "Connecting to host :", host
	print "Connecting to db:", db
	trandb= mdb.connect(host=host, user=user , passwd=passwd, db=db)
	trandb.autocommit(auto_commit)
	return trandb

def ep_institute_subscription_curation():
	print "producing institute wise ebsco n proquest subscription and curation stats"
	mysqlcon=mysql_con()
	cursor=mysqlcon.cursor()
	print "deleting existing institute wise subscription and curation counts"
	cursor.execute('delete from tmp.ep_institute_subscription_curation;')
	print "inserting fresh institute wise subscription and curation counts"
	cursor.execute('select distinct attr_value from config_repo_arp.config_repo where attr_key = "db";')
	db_data=cursor.fetchall()
	
	for db in db_data:
		cursor.execute('select sc.source_id,sc.subsource_id,sc.subsource from {0}.subsource_permission ins join centralized_content.subsource sc on ins.subsource_id=sc.subsource_id'.format(db[0]))
		subsource_list=cursor.fetchall()
		cursor.execute('select distinct institute from config_repo_arp.config_repo where attr_value="{0}";'.format(db[0]))
		institute=cursor.fetchone()
		for subsource in subsource_list:
			cursor.execute('select count(distinct dc.doc_seq_id) count from centralized_content.document dc join {0}.learning_path_item lpi on lpi.doc_seq_id=dc.doc_seq_id where dc.subsource_id={1} and dc.deleted=0 and lpi.deleted=0 group by dc.subsource_id;'.format(db[0],subsource[1]))
			curated_count=cursor.fetchone()
			if not curated_count:
				cursor.execute('insert into tmp.ep_institute_subscription_curation values ("{0}","{1}","{2}","{3}",0);'.format(institute[0],subsource[0],subsource[1],subsource[2]))
			else:
				cursor.execute('insert into tmp.ep_institute_subscription_curation values ("{0}","{1}","{2}","{3}","{4}");'.format(institute[0],subsource[0],subsource[1],subsource[2],curated_count[0]))
	mysqlcon.close()

def ep_subsource_yearwise_count():
	print 'running solr yearwise queries to get subsource based record count'
	start_year=1898
	end_year=2021
	from_year=0
	to_year=0
	mysqlcon=mysql_con()
	cursor=mysqlcon.cursor()
	print 'deleting existing solr subsource wise counts'
	cursor.execute('delete from tmp.ep_subsource_yearwise_count')
	print 'inserting fresh solr subsource wise counts'
	for year in range(start_year,end_year,1):
		from_year=year
		to_year=year+1
		solr_query_output=requests.get("http://10.54.199.117:8983/solr/emr-20190420/select?facet.field=subsource_id&facet=on&fq=publish_date:[{0}-12-31T23:59:59Z TO {1}-12-31T23:59:59Z]&fq=source_id:[101 TO 102]&q=*:*&rows=0&wt=xml".format(from_year,to_year))
		time.sleep(10)
		soup=BeautifulSoup(solr_query_output.text,"html.parser")
		rec_count_subsource=soup.find('lst',{'name':'subsource_id'}).find_all('int')
		for rec in rec_count_subsource:
			cursor.execute('insert into tmp.ep_subsource_yearwise_count values ("{0}","{1}","{2}","{3}");'.format(rec.get('name'),rec.get_text(),from_year,to_year))
	mysqlcon.close()

def ep_preliminary_stats():
	mysqlcon=mysql_con()
	cursor=mysqlcon.cursor()
	print "deleting existing preliminary stats"
	cursor.execute('delete from tmp.ep_preliminary_stats;')
	print "inserting fresh preliminary stats"
	cursor.execute('insert into tmp.ep_preliminary_stats(subsource_id,source_id,subscribed_by,curated_count) select distinct subsource_id,source_id,count(institute) subscribed_by,sum(curate_count) curated_count from tmp.ep_institute_subscription_curation group by subsource_id;')
	df = pd.read_sql('select subsource_id,source_id,subscribed_by,curated_count from tmp.ep_preliminary_stats;',mysqlcon)
	df['priority_subscribed_by']=df.groupby(['source_id'])['subscribed_by'].rank(ascending=False)
	df['priority_subscribed_by']=df['priority_subscribed_by'].astype(int)
	df['priority_curated_count']=df.groupby(['source_id'])['curated_count'].rank(ascending=False)
	df['priority_curated_count']=df['priority_curated_count'].astype(int)
	df['product_priority']=df['priority_subscribed_by']*df['priority_curated_count']
	df['crawl_priority']=df.groupby(['source_id'])['product_priority'].rank(method ='dense',ascending=True)
	mysqlcon.close()
	return df

def insert_data_into_mysqldb(dataframe, creds, table, mode='append'):
    eng_str = 'mysql+mysqldb://{0}:{1}@{2}/{3}'.format(creds['MYSQL_USER'],
                                                       creds['MYSQL_PASSWD'],
                                                       creds['MYSQL_HOST'],
                                                       creds['MYSQL_DB'])
    engine = create_engine(eng_str, echo=False)

    dataframe.to_sql(name='{}'.format(table),
                     con=engine, if_exists=mode, index=False)

def cleanup_ep_final_stats():
	mysqlcon=mysql_con()
	cursor=mysqlcon.cursor()
	print 'adding additional columns and data to produce clean table with stats'
	cursor.execute('alter table ep_final_stats drop column priority_subscribed_by;')
	cursor.execute('alter table ep_final_stats drop column priority_curated_count;')
	cursor.execute('alter table ep_final_stats drop column product_priority;')
	cursor.execute('alter table ep_final_stats add column subsource varchar(100);')
	cursor.execute('alter table ep_final_stats change column subsource subsource varchar(100) after subsource_id;')
	cursor.execute('update ep_final_stats efs join centralized_content.subsource sc on efs.subsource_id=sc.subsource_id set efs.subsource=sc.subsource;')
	cursor.execute('alter table ep_final_stats add column total_record_count int(11);')
	cursor.execute('alter table ep_final_stats change column total_record_count total_record_count int(11) after curated_count;')
	cursor.execute('update ep_final_stats efs join (select subsource_id,sum(rec_count) total_rec_count from ep_subsource_yearwise_count group by subsource_id) esyc on efs.subsource_id=esyc.subsource_id set efs.total_record_count=esyc.total_rec_count;')
	mysqlcon.close()
	
def ep_final_stats():
	MYSQL_HOST = "se-db.prod-se.aws.learningace.com"
	MYSQL_USER = "root"
	MYSQL_PASSWD = "aceuser123"
	MYSQL_DB = "tmp"

	creds = dict(MYSQL_HOST=MYSQL_HOST,
	             MYSQL_USER=MYSQL_USER,
	             MYSQL_PASSWD=MYSQL_PASSWD,
	             MYSQL_DB=MYSQL_DB)

	df = ep_preliminary_stats()
	insert_data_into_mysqldb(df, creds, 'ep_final_stats', mode='replace')
	cleanup_ep_final_stats()

def main():
	ep_institute_subscription_curation()
	ep_subsource_yearwise_count()
	ep_final_stats()
	print "refer tmp.ep_final_stats table for crawl_priority"

if __name__ == '__main__':
	main()
