import MySQLdb
import sys
import datetime
import re
import _mysql

def example():
	try:
		conn = MySQLdb.connect(host="127.0.0.1", user="root", passwd="freeschool", db="amazon")
		cur = conn.cursor()
		cur.execute("SELECT count(*) FROM articles")
		count = cur.fetchone()
		print count
	except MySQLdb.Error, e:
		print "Error %d: %s" % (e.args[0], e.args[1])
		sys.exit(1)
	finally:
		if conn:
			conn.close()

def print_dict(dictionary):
	f = open("test.txt", "a")
	for v in dictionary.values():
		f.write(str(v))
		f.write("\n")
	f.write("\n\n")
	f.close()

def articles(amazon_conn, inmotion_conn, des_conn, table_name):
	amazon_cur = amazon_conn.cursor(MySQLdb.cursors.DictCursor)
	inmotion_cur = inmotion_conn.cursor(MySQLdb.cursors.DictCursor)
	des_cur = des_conn.cursor()

	if table_name == 'pages':
		amazon_cur.execute("SELECT id, article_id, page_no, title, body, citation,\
		created_at, updated_at, image_file_name, image_content_type,\
		image_file_size, image_updated_at FROM %s" % table_name)
	else:
		amazon_cur.execute("SELECT * FROM %s" % table_name)

	articles_in_amazon = amazon_cur.fetchall()
	dict_amazon = {}
	for article in articles_in_amazon:
		if table_name == "articles":		
			dict_amazon[article["title"]] = article
		elif table_name == "pages":
			dict_amazon[str(article["article_id"]) + " " + str(article["page_no"])] = article
		elif table_name == "users":
			dict_amazon[article["user_name"]] = article
			#dict_amazon[article["id"]] = article
		elif table_name == "publishers":
			dict_amazon[article["user_id"]] = article

	print_dict(dict_amazon)

	if table_name == 'pages':
		inmotion_cur.execute("SELECT id, article_id, page_no, title, body, citation,\
		created_at, updated_at, image_file_name, image_content_type,\
		image_file_size, image_updated_at FROM %s" % table_name)
	else:
		inmotion_cur.execute("SELECT * FROM %s" % table_name)

	articles_in_inmotion = inmotion_cur.fetchall()
	dict_inmotion = {}
	for article in articles_in_inmotion:		
		if table_name == "articles":		
			dict_inmotion[article["title"]] = article
		elif table_name == "pages":
			dict_inmotion[str(article["article_id"]) + " " + str(article["page_no"])] = article
		elif table_name == "users":
			#dict_inmotion[article["id"]] = article
			dict_inmotion[article["user_name"]] = article
		elif table_name == "publishers":
			dict_inmotion[article["user_id"]] = article

	print_dict(dict_inmotion)

	merge_dict = {}
	for title in dict_amazon.keys():
		if title in dict_inmotion:
			if dict_amazon[title]["updated_at"] > dict_inmotion[title]["updated_at"]:
				merge_dict[title] = dict_amazon[title]
			else:
				merge_dict[title] = dict_inmotion[title]

			if table_name == "users":
				if dict_amazon[title]["user_name"] != dict_inmotion[title]["user_name"]:
					print dict_amazon[title]["user_name"], "_", dict_amazon[title]["id"]
					print dict_inmotion[title]["user_name"], "_", dict_inmotion[title]["id"]

		else:
			merge_dict[title] = dict_amazon[title]

	for title in dict_inmotion.keys():
		if title in dict_amazon:
			if dict_amazon[title]["updated_at"] > dict_inmotion[title]["updated_at"]:				
				merge_dict[title] = dict_amazon[title]
			else:
				merge_dict[title] = dict_inmotion[title]

			if table_name == "users":
				if dict_amazon[title]["user_name"] != dict_inmotion[title]["user_name"]:
					print dict_amazon[title]["user_name"], "_", dict_amazon[title]["id"]
					print dict_inmotion[title]["user_name"], "_", dict_inmotion[title]["id"]

		else:
			merge_dict[title] = dict_inmotion[title]

	print_dict(merge_dict)
	if len(merge_dict) > 0:
		column_name = "(%s)" %", ".join(merge_dict[merge_dict.keys()[0]].keys())		

		count = 0
		query = "INSERT INTO %s %s VALUES " % (table_name, column_name)		

		black_list = [1776, 1777]

		for v in merge_dict.values():		
			if table_name == 'users':
				if v['id'] in black_list:
					continue
			if table_name == 'publishers':
				if v['user_id'] in black_list:
					continue

			if count % 10 == 1:
				query = query[:-2]						
				#print query
				des_cur.execute(query)
				des_conn.commit()		
				query = "INSERT INTO %s %s VALUES " % (table_name, column_name)
				part_of_query = ""
			
			part_of_query = ""
			for v in v.values():
				if (type(v) == str or isinstance(v, datetime.datetime)):
					part_of_query = ", ".join([part_of_query, "\"%s\""% _mysql.escape_string(str(v))])	
				elif v == None:
					part_of_query = ", ".join([part_of_query, "NULL"])	
				else:
					part_of_query = ", ".join([part_of_query, str(v)])

			part_of_query = part_of_query[1:]
			part_of_query = "".join(["(", part_of_query, "), "])			
			query = " ".join([query, part_of_query])					

			count += 1

		query = query[:-2]	
		#print query	
		des_cur.execute(query)
		des_conn.commit()

def merge():
	try:
		amazon_conn = MySQLdb.connect(host="127.0.0.1", user="root", passwd="freeschool", db="amazon")
		inmotion_conn = MySQLdb.connect(host="127.0.0.1", user="root", passwd="freeschool", db="inmotion")
		destination_conn = MySQLdb.connect(host="127.0.0.1", user="root", passwd="freeschool", db="merge_test")
		#articles(amazon_conn, inmotion_conn, destination_conn, "articles")
		articles(amazon_conn, inmotion_conn, destination_conn, "pages")
		#articles(amazon_conn, inmotion_conn, destination_conn, "users")
		#articles(amazon_conn, inmotion_conn, destination_conn, "publishers")
	except MySQLdb.Error, e:
		print "Error %d: %s" % (e.args[0], e.args[1])
		sys.exit(1)
	finally:
		if amazon_conn:
			amazon_conn.close()
		if inmotion_conn:
			inmotion_conn.close()		

if __name__ == "__main__":
	merge()
