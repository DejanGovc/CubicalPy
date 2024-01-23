from functools import partial
from multiprocessing import Pool
import os
import time
import sqlite3 as s3
import builtins

import user_interface as ui
import functions as fn

# REDEFINE print FUNCTION TO FLUSH AUTOMATICALLY.
def print(*args, **kwargs):
    kwargs['flush'] = True
    builtins.print(*args, **kwargs)

def main():
    ### SET PARAMETERS
    n, chunksize, dbprefix, surf_type = ui.user_interface()
    n2 = n*(n-1)*2**(n-3)
    imin = 2 # default: 2
    imax = n2 # default: n2
    cplxs = [1]
    lencplxs = 1
    if dbprefix == "db":
        extend_fun = partial(fn.disconnected_withbdry_extendonce,n=n)
    elif dbprefix == "b":
        extend_fun = partial(fn.withbdry_extendonce,n=n)
    elif dbprefix == "dc":
        extend_fun = partial(fn.disconnected_extendonce,n=n)
    else:
        extend_fun = partial(fn.extendonce,n=n)
    label_fun = partial(fn.cubicalcanlabel,n=n)
    bool_fun = partial(fn.testedges,n=n)

    db_name = dbprefix+f'cplxs{n}.db'
    if os.path.exists(db_name):
        response = input(f"WARNING!!! Database file {db_name} already exists.\nOverwrite [O], continue existing computation [C] or quit [Q]? ").lower()
        if response == 'o':
            print("Overwriting database file.")
            os.remove(db_name)
        elif response == "c":
            print("Resuming computation ...",end="\r")
        else:
            print("Cannot proceed without permission. Exiting computation.")
            exit()

    ### CREATE AND INITIALIZE DATABASE (OR ATTEMPT TO CONTINUE PREVIOUS COMPUTATION)
    t0 = time.time()

    conn = s3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute("PRAGMA integrity_check")
    if cursor.fetchone()[0] != "ok":
        print("ERROR: Integrity of the database has been compromised. Exiting computation.")
        exit()

    #cursor.execute("PRAGMA journal_mode = WAL")
    #cursor.execute("PRAGMA synchronous = NORMAL")
    #cursor.execute("PRAGMA journal_size_limit = 6144000")
    #cursor.execute("PRAGMA cache_size = -32000")
    #cursor.execute("PRAGMA temp_store = MEMORY")    
    #cursor.execute("PRAGMA mmap_size = 30000000000")

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tab = [s for t in cursor.fetchall() for s in t]
    if tab != []:
        tab1 = [eval(s[5:]) for s in tab if s[:5] == "cplxs"]
        if tab1 == []:
            print("The computation is already complete. Exiting computation.")
            exit()
        imin = max(tab1)+1
        table_name = "cplxs"+str(imin-1)
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        lencplxs = cursor.fetchone()[0]
        for t in tab:
            if t != table_name and t != "goodcplxs":
                cursor.execute(f"DROP TABLE IF EXISTS {t}")
    
    if imin == 2:
        cursor.execute("BEGIN TRANSACTION")
        cursor.execute("DROP TABLE IF EXISTS cplxs1")
        cursor.execute("DROP TABLE IF EXISTS goodcplxs")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS goodcplxs (
                id INTEGER PRIMARY KEY,
                integer1 BLOB
            )
        """)
        if dbprefix == "b" or dbprefix == "db":
            cursor.execute("INSERT INTO goodcplxs (integer1) VALUES (?)", 
                        (fn.int_to_blob(cplxs[0],n2),))
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cplxs1 (
                id INTEGER PRIMARY KEY,
                integer1 BLOB
            )
        """)
        for val1 in cplxs:
            cursor.execute("INSERT INTO cplxs1 (integer1) VALUES (?)", 
                        (fn.int_to_blob(val1,n2),))
        conn.commit()

    print(f"Enumerating {surf_type}; n = {n}, chunk size = {chunksize}.\nStoring data to database {db_name}.")

    write_to_text = True ### CHANGE THIS TO False TO DISABLE .txt OUTPUT
    print("WARNING!!! Also storing final result as a .txt file.")

    ### MAIN LOOP OF THE COMPUTATION (STARTING FROM POINT OF INTERRUPTION IF NEEDED)
    for i in range(imin,imax+1):

        table_name = f"cplxs{i-1}"
        cursor.execute(f"SELECT * FROM {table_name} ORDER BY id")
        labeling_cursor = conn.cursor()
        cc = 0
        ctot = lencplxs//chunksize+1
        lencplxs = 0
        while True:
            cc += 1
            rows = cursor.fetchmany(chunksize)
            if not rows:
                break
            cplxs = [fn.blob_to_int(r[1]) for r in rows]
            labeling_cursor.execute("""
                CREATE TABLE IF NOT EXISTS labeledcplxs (
                    id INTEGER PRIMARY KEY,
                    integer1 BLOB,
                    integer2 BLOB,
                    integer3 INTEGER NOT NULL CHECK (integer3 IN (0, 1)),
                    integer4 INTEGER NOT NULL CHECK (integer3 IN (0, 1)))
            """)
            with Pool() as p:
                cplxs = p.map(extend_fun,cplxs)
            cplxs = [c for cxs in cplxs for c in cxs]
            lencplxs += len(cplxs)
            print(" "*120,end="\r")
            print(time.ctime()+f": Computing chunk {cc}/{ctot}. Done extending ({len(cplxs)}). ",end="")
            with Pool() as p:
                labels = p.map(label_fun, cplxs)
            print("Done labeling. ",end="")
            with Pool() as p:
                bool = p.map(bool_fun, cplxs)
            bool1 = [b[0] and b[1] for b in bool]
            bool2 = [b[0] and not b[1] for b in bool]
            if dbprefix == "b" or dbprefix == "db":
                bool1 = [b[0] for b in bool]
                bool2 = bool1
            if dbprefix == "dc":
                bool1 = [b[0] and b[1] for b in bool]
                bool2 = [b[0] for b in bool]
            print("Done testing.",end="\r")
            for val1, val2, val3, val4 in zip(cplxs, labels, bool1, bool2):
                labeling_cursor.execute("INSERT INTO labeledcplxs (integer1, integer2, integer3, integer4) VALUES (?, ?, ?, ?)", 
                            (fn.int_to_blob(val1,n2), fn.int_to_blob(val2,n2), val3, val4))
        labeling_cursor.close()
        cursor.execute(f"""
            DROP TABLE IF EXISTS cplxs{i-1}
        """)
        conn.commit()
        cursor.execute("VACUUM")
                
        print(" "*120,end="\r")
        print(time.ctime()+f": All chunks extended, labeled & tested ({lencplxs}). Data stored.",end="\r")

        cursor.execute("CREATE INDEX idx_integer2_id ON labeledcplxs(integer2, id)")

        cursor.execute("""
            CREATE TABLE uniquecplxs AS
            SELECT l.id, l.integer1, l.integer2, l.integer3, l.integer4
            FROM labeledcplxs l
            INNER JOIN (
                SELECT integer2, MIN(id) as min_id
                FROM labeledcplxs
                GROUP BY integer2
            ) as subquery
            ON l.integer2 = subquery.integer2 AND l.id = subquery.min_id
        """)
        cursor.execute("""
            DROP TABLE IF EXISTS labeledcplxs
        """)
        conn.commit()
        cursor.execute("SELECT COUNT(*) FROM uniquecplxs")
        lenunique = cursor.fetchone()[0]
        print(" "*120,end="\r")
        print(time.ctime()+f": Done reducing ({lenunique}). ",end="")

        table_name = f"cplxs{i}"
        cursor.execute("BEGIN TRANSACTION")
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY,
                integer1 BLOB
            )
        """)
        cursor.execute(f"""
            INSERT INTO {table_name} (integer1)
            SELECT integer1
            FROM uniquecplxs
            WHERE integer4
            ORDER BY id;
        """)
        cursor.execute(f"""
            INSERT INTO goodcplxs (integer1)
            SELECT integer1
            FROM uniquecplxs
            WHERE integer3
            ORDER BY id;
        """)
        cursor.execute("""
            DROP TABLE IF EXISTS uniquecplxs
        """)
        print("Data stored.",end="\r")
        conn.commit()
        cursor.execute("VACUUM")

        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        lencplxs = cursor.fetchone()[0]
        cursor.execute(f"SELECT COUNT(*) FROM goodcplxs")
        lengoodcplxs = cursor.fetchone()[0]

        print(time.ctime()+": # squares: "+str(i)+"; # cplxs: "+str(lencplxs)+"; # goodcplxs: "+str(lengoodcplxs))
        if lencplxs == 0:
            cursor.execute(f"""
                DROP TABLE IF EXISTS cplxs{i}
            """)
            print("Vacuuming ...",end="\r")
            cursor.execute("VACUUM")
            break
    print("Total number of goodcplxs: "+str(lengoodcplxs))

    if write_to_text:
        cursor.execute("SELECT * FROM goodcplxs ORDER BY id")
        writecplxs = cursor.fetchall()
        writestring = "".join([str(fn.fromzeroone(fn.blob_to_int(c[1]),fn.cubes(n,2)))+"\n" for c in writecplxs])
        g = open(db_name[:-3]+"_result.txt","w")
        g.write(writestring)
        g.close()
    cursor.close()
    conn.close()

    print("Total time of computation: "+str(time.time()-t0))

if __name__ == "__main__":
    main()