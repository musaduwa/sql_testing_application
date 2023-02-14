import numpy as np
from time import time, sleep
import cx_Oracle
import os

# Database connection
DB_NAME = "dbname"
DB_USER = "dbuser"
DB_PASS = "password"

# Paths
REPORT_OUTPUT = "transactions_reporttxt"
PATH = "query"


def load_queries(PATH):
    analyse_dict = {}
    for query in os.listdir(PATH):
        with open(os.path.join(PATH, query), 'r') as f:
            analyse_dict[query] = [f.read().split(";")[:-1]]
    return analyse_dict


def main(n):
    # Connect string format: [username]/[password]@//[hostname]:[port]/[DB service name]
    conn = cx_Oracle.connect("%s/%s@//localhost:1521/%s" %
                             (DB_USER, DB_PASS, DB_NAME))
    cur = conn.cursor()

    analyse_dict = load_queries(PATH)
    with open(REPORT_OUTPUT, "w") as f:
        for key in analyse_dict:
            print("START", key)
            analyse_dict[key].append(get_explain(cur, analyse_dict[key][0]))
            analyse_dict[key].append(
                [execute_statements(cur, analyse_dict[key][0]) for _ in range(n)])
            analyse_dict[key].append(analyse(analyse_dict[key][2]))
            print("DONE")

            f.write(generate_report(key, analyse_dict[key]))
            f.write("%s\n\n" % "".join("#" for _ in range(150)))

    return analyse_dict


def analyse(times):
    return (len(times), np.mean(times), np.var(times), np.min(times), np.max(times))


def get_explain(cur, query):
    cur.execute("EXPLAIN PLAN FOR " + query[0])
    cur.execute("SELECT * FROM table(DBMS_XPLAN.DISPLAY)")
    plans = cur.fetchall()
    if len(query) > 1:
        for q in query[1:]:
            cur.execute(q)
    string = ''
    for i in plans:
        string += i[0] + '\n'
    return string


def generate_report(key, items):
    rap = "SCRIPT NAME: %s \n Number of runs: %d  \n Avg Execution Time: %f \n Var Execution Time: %f " \
          "  \n Min Execution Time: %f   \n Max Execution Time: %f \n \nQUERY: \n %s  \nQUERY PLAN: \n %s \n " % (
              key, items[3][0], items[3][1], items[3][2], items[3][3], items[3][4], items[0][0], items[1])
    return rap


def execute_statements(cur, queries):
    start = time()
    for query in queries:
        cur.execute(query)
        # if query.startswith('INSERT', 'DELETE', 'UPDATE'):
        #     cur.execute('ROLLBACK')
    cur.execute('ALTER SYSTEM FLUSH BUFFER_CACHE')
    return time() - start


x = main(5)
