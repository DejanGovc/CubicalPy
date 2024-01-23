from functools import partial
from multiprocessing import Pool
import time
import functions as fn

def main():
    print("Started computation.")
    n = 5 # DIMENSION OF CUBE, HAS TO BE SET MANUALLY.
    n2 = n*(n-1)*2**(n-3)
    tab = fn.cubes(n,2)
    cplxs = [fn.zeroone([tuple(tab[0])],tab)]
    goodcplxs = []
    bool1_fun = partial(fn.zeroonevalidq,tab=tab)
    bool2_fun = partial(fn.zeroonesaturatedq,tab=tab)
    print("Variables loaded.")
    for i in range(-1+1,n2):
        cplxs = fn.cubicalextend(cplxs,tab,n)
        #print(time.ctime()+": Done extending.")
        cplxs = fn.cubicalisoreduce(cplxs,tab,n)
        with Pool() as p:
            bool1 = p.map(bool1_fun, cplxs)
            bool1 = [b for b in bool1]
            bool2 = p.map(bool2_fun, cplxs)
            bool2 = [b for b in bool2]    
        bool3 = [val1 and val2 for val1, val2 in zip(bool1,bool2)]
        bool4 = [val1 and not val2 for val1, val2 in zip(bool1,bool2)]
        goodcplxs += [c for c, val in zip(cplxs, bool3) if val]
        cplxs = [c for c, val in zip(cplxs, bool4) if val]
        print(time.ctime()+": # squares: "+str(i+2)+"; # cplxs: "+str(len(cplxs))+"; # goodcplxs: "+str(len(goodcplxs)))
        if cplxs == []:
            break
    print("Total number of goodcplxs: "+str(len(goodcplxs)))
    writestring = "".join([str(c)+"\n" for c in goodcplxs])
    g = open("cplxs"+str(n)+".txt","w")
    g.write(writestring)
    g.close()

if __name__ == "__main__":
    main()