import pandas as pd
import cx_Oracle

#feed='346|GCS_BQ'

#Oracle Connection Details

def create_json(feed):

	dsn_tns = cx_Oracle.makedsn('35.243.168.164', '1521', service_name='pdborcl.us_east1_b.c.juniperonprem.internal')
	conn = cx_Oracle.connect(user='micro', password='services', dsn=dsn_tns)
	c=conn.cursor()
	
	feed_seq=feed.split('|')[0]
	feed_name=feed.split('|')[1]
	
	c.execute("SELECT DISTINCT 																					\
				LOWER(CONCAT(CONCAT(SUBSTR(pub_feed.pub_feed_name,INSTR(pub_feed.pub_feed_name,':')+1,LENGTH(pub_feed.pub_feed_name)-INSTR(pub_feed.pub_feed_name,':')-4),'.'),feed_dtls.tgt_tbl_name)) AS dataset_table,	\
				LOWER(pub_feed.feed_src_bkt),																	\
				CONCAT(CONCAT(pub_feed.feed_src_type,':'),LOWER(pub_feed.pub_feed_name)) 		AS source_table \
			FROM juniper_project_master pro_master																\
			INNER JOIN juniper_pub_feed_dtls pub_feed															\
			ON pro_master.project_sequence=pub_feed.project_sequence											\
			INNER JOIN (																						\
				SELECT DISTINCT 																				\
					pub_feed_sequence , feed_unique_name                                                        \
				FROM juniper_pub_feed_status                                                                    \
				WHERE UPPER(status)='SUCCESS') s                                                                \
			ON pub_feed.pub_feed_sequence=s.pub_feed_sequence                                                   \
			INNER JOIN juniper_pub_feed_file_dtls feed_dtls                                                     \
			ON feed_dtls.pub_feed_sequence=pub_feed.pub_feed_sequence                                           \
			WHERE pub_feed.ext_feed_sequence='" + feed_seq + "'")

	data=c.fetchall()
	conn.close()
	
	#Preparing Json Object
	
	if len(data) != 0:
		df = pd.DataFrame(data, columns = ['Dataset_Table', 'Bucket', 'Source_Table_Name'])
		prepare_child_tag=""
		for i in range(len(df)):
		 prepare_child_tag=prepare_child_tag + "{'name': 'BQ:" + df['Dataset_Table'][i] + "','level':'orange','icon': '/assets/img/bigquery.png','children': [{'name': 'GCS:"+ df['Bucket'][i] + "','level':'blue','icon': '/assets/img/storage.png','children': [{'name': '"+ df['Source_Table_Name'][i] + "','level':'blue','icon': '/assets/img/source.png'}]}]},"
		
		childern_tags= prepare_child_tag[:-1]
		#feed_name=str(df['Feed_Name'].unique()[0])
		json_final="[{'name':'Feed " + feed_name + "','icon':'/assets/img/feed.png','children': [" +  childern_tags + "]}]"
		
		#Updating HTML
		
		temp_html = open('/home/ms/backend/lineage/template.html','r').read()
		temp_html=str(temp_html)
		temp_html = temp_html.replace('BIG_TREE',json_final)
		
		html=open('/home/ms/backend/lineage/index.html','w')
		html.write(temp_html)
		html.close()
	else:
		temp_html=""
	
	return temp_html