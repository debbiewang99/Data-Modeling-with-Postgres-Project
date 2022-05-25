Purpose of database: to analyze the data Sparkify has been collecting on songs and user activity on their music streaming app and create a Postgres database that is easy to query.


To run the Python scripts, I wrote the lines:

!python create_tables.py 

in etl.ipynb, and the line:

!python etl.py

in test.ipynb, so just running the cell should run the Python scripts. 


Explanation of files:
- data: contains the Sparkify data from their music streaming app

- create_tables.py: has functions to create and connect to the sparkifydb, drop and creation functions (using functions from sql_queries.py)
*Note: drop_tables may be commented out because it at times causes error:
*1. initially when the tables do not yet exist
*2. when running etl.ipynb and create_tables.py is called and tries to drop the tables, but it does not exist:
Traceback (most recent call last):
  File "create_tables.py", line 71, in <module>
    main()
  File "create_tables.py", line 63, in main
    drop_tables(cur, conn)
  File "create_tables.py", line 35, in drop_tables
    cur.execute(query)
psycopg2.ProgrammingError: table "songplays" does not exist
^when this error occurs, drop_tables is commented out

- data.zip: created to download the files to my local device and upload it to Github, contains file "data"

- etl.ipynb: walksthrough ETL process for creating tables in the database
	creates songplays, artist, time, users, and songs tables
	
- sql_queries.py: inclues calls for dropping, creating, and song selection

- test.ipynb: tests the creation and specifics of adding to the database to determine correctness
	
	
Database schema design and ETL pipeline:
	