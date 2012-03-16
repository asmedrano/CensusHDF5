import psycopg2


def main():
	#connect to db
	conn = psycopg2.connect("dbname=communityprofiles user=angel")
	cur = conn.cursor()
	cur2 = conn.cursor()

	# get 1972 geographies
	cur.execute("SELECT name, geo_id FROM profiles_georecord LIMIT 1972")

	for geo in cur:
		logrecno = geo[1]
		print geo[0], logrecno

		query_string = "SELECT col10 FROM census_row WHERE filetype = '2010e5' AND logrecno = '%s'  LIMIT 1" % logrecno

		# Rhode Island has 1973 geo records based on the geographies file from acs. Lets try runing this
		cur2.execute(query_string)

		for record in cur2:	
			print record



	cur.close()
	conn.close()

	



if __name__ == '__main__':
	main()