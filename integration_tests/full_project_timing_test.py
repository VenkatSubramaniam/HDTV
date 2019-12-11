import os
import pickle
import psycopg2
import timeit

conn = psycopg2.connect(dbname="postgres", user="postgres", password="")
conn.set_isolation_level(0)
cur = conn.cursor()


vals = [1e2,5e2,1e3,5e3,2e4,5e4,75e3,1e5,2e5,25e4,3e5,35e4,45e4,5e5,6e5,75e4,1e6,15e5,2e6,5e6,1e7]
file_map = {key: f"{key}_test.csv" for key in vals}
os.chdir("../")
t_d = {k:[] for k in vals}
for n in vals:
	for i in range(30):
		cur.execute("""DROP TABLE IF EXISTS PEOPLE;""")
		t0 = timeit.default_timer()
		os.system(f"python veranda.py -f ./util/{file_map[n]} -c appln_filing_date appln_filing_year appln_id -U postgres -D postgres -P password -p 5432")
		t_d[n].append(timeit.default_timer() - t0)
		#Try with just the single drop existing

with open('./util/full_project_dump.pickle', 'wb') as handle:
		pickle.dump(t_d,handle,protocol=pickle.HIGHEST_PROTOCOL)