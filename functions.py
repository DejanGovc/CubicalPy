#import time
from multiprocessing import Pool
from collections import Counter
from functools import lru_cache
from functools import partial
from itertools import product
from itertools import groupby
from itertools import permutations

@lru_cache(maxsize=None)
def cubes(n,k=None):
    prod = product(range(3),repeat=n)
    if k is not None:
        return tuple(c for c in prod if c.count(2) == k)
    else:
        return tuple(prod)

@lru_cache(maxsize=None)
def bdry(c):
    b = []
    for i in range(len(c)):
        if c[i] == 2:
            b.append(c[:i]+(0,)+c[i+1:])
            b.append(c[:i]+(1,)+c[i+1:])
    return tuple(b)

@lru_cache(maxsize=None)
def bbdry(c):
    return tuple(sorted(list(set(v for s in bdry(c) for v in bdry(s)))))

def edgedegs(cplx):
    return list(Counter([e for c in cplx for e in bdry(c)]).values())

def badedges(cplx):
    return sorted([c for c,val in Counter([e for c in cplx for e in bdry(c)]).items() if val < 2])

### PERMUTATIONS:

"""def padding(t,l):
    return tuple(-1 for i in range(l-len(t)))+t

def sorting_perm(lst):
    l = max([len(t) for t in lst])
    extlist = [padding(t,l) for t in lst]
    extlist = [extlist[i]+(i,) for i in range(len(lst))]
    perm = [extlist.index(v) for v in sorted(extlist)]
    return perm

def repeats(lst):
    perm = sorting_perm(lst)
    lst = [lst[p] for p in perm]
    ordlist = sorted(set([lst.index(v) for v in lst]))
    ordlist = ordlist+[len(lst)]
    return [ordlist[i+1]-ordlist[i] for i in range(len(ordlist)-1)]

@lru_cache(maxsize=None)
def perms(rptlist):
    permlist = [list(permutations(range(p))) for p in rptlist]
    for i in range(len(rptlist)-1):
        m = max(permlist[-2][0])
        permlist[-1] = [[j+m+1 for j in s] for s in permlist[-1]]
        permlist[-2] = [list(p)+list(s) for p in permlist[-2] for s in permlist[-1]]
        permlist = permlist[:-1]
    return permlist[0]"""

def sorting_perm(lst):
    sorted_pairs = sorted(enumerate(lst), key=lambda x: x[1])
    permutation = tuple(index for index, _ in sorted_pairs)
    return permutation

def repeats(lst):
    tab = sorted(lst)
    counts = tuple(len(list(group)) for _, group in groupby(tab))
    return counts
    
@lru_cache(maxsize=None)
def perms(rptlist):
    permlist = [list(permutations(range(p))) for p in rptlist]
    for i in range(len(rptlist)-1):
        m = max(permlist[-2][0])
        permlist[-1] = [[j+m+1 for j in s] for s in permlist[-1]]
        permlist[-2] = [list(p)+list(s) for p in permlist[-2] for s in permlist[-1]]
        permlist = permlist[:-1]
    return tuple(tuple(p) for p in permlist[0])

### AUTOMORPHISMS:

def ddegs(cplx):
    vertices = cubes(len(cplx[0]),0)
    squares = sorted(cplx)
    edges = sorted(list(set([t for c in cplx for t in bdry(c)])))
    vine = Counter([t for c in edges for t in bdry(c)])
    vins = Counter([t for c in squares for t in bbdry(c)])
    combined = [(vine.get(v,0),vins.get(v,0)) for v in vertices]
    return combined

@lru_cache(maxsize=None)
def nbhd(v):
    return tuple(v[:i]+(1-v[i],)+v[i+1:] for i in range(len(v)))

def bigdegs(cplx):
    vertices = cubes(len(cplx[0]),0)
    dd = ddegs(cplx)
    ddict = dict(zip(vertices,dd))
    return [ddict[v]+tuple(i for t in sorted([ddict[u] for u in nbhd(v)]) for i in t) for v in vertices]

def bigbigdegs(cplx):
    vertices = cubes(len(cplx[0]),0)
    dd = bigdegs(cplx)
    ddict = dict(zip(vertices,dd))
    return [ddict[v]+tuple(i for t in sorted([ddict[u] for u in nbhd(v)]) for i in t) for v in vertices]

def pairs(dd,n):
    vertices = cubes(n,0)
    mindeg = sorted(dd)[0]
    ddict = dict(zip(vertices,dd))
    minverts = [v for v,val in ddict.items() if val == mindeg]
    pp = []
    for v in minverts:
        nbdegs = [ddict[u] for u in nbhd(v)]
        pi = sorting_perm(nbdegs)
        permlist = [tuple(pi[i] for i in tau) for tau in perms(repeats(nbdegs))]
        pp += [(v,tau) for tau in permlist]
    return tuple(pp)

@lru_cache(maxsize=None)
def renamecube(c,p):
    newc = tuple(1-c[i] if p[0][i] == 1 and c[i] <= 1 else c[i] for i in range(len(c)))
    return tuple(newc[i] for i in p[1])

def rename(cplx,p):
    return [renamecube(c,p) for c in cplx]

### DIFFERENT ENCODINGS OF COMPLEXES:

def zeroone(lst,tab):
    zo = 0
    for e in lst:
        zo += 2**(tab.index(e))
    return zo

def fromzeroone(zo,tab):
    fromzo = []
    lst = [int(bit) for bit in bin(zo)[2:]]
    lst.reverse()
    lst += [0 for i in range(len(tab)-len(lst))]
    for i in range(len(lst)):
        if lst[i] == 1:
            fromzo.append(tab[i])
    return(sorted(fromzo))

### EXTENSION AND REDUCTION:

def extendonce(ncplx,tab,n):
    ncplxsnew = []
    cplx = fromzeroone(ncplx,tab)
    e = badedges(cplx)[0]
    for i in reversed(range(n)):
        f = e[:i]+(2,)+e[i+1:]
        if f == e:
            continue
        if f in cplx:
            continue
        ncplxsnew += [zeroone(cplx + [f],tab)]
    return ncplxsnew

def cubicalextend(ncplxs,tab,n):
    extendfun = partial(extendonce,tab=tab,n=n)
    with Pool() as p:
        ncplxsnew = p.map(extendfun,ncplxs)
    ncplxsnew = [c for ncplxs in ncplxsnew for c in ncplxs]
    return ncplxsnew

def cubicalcanlabel(ncplx,tab,n):
    cplx = fromzeroone(ncplx,tab)
    dd = bigbigdegs(cplx)
    prs = pairs(dd,n)
    #print(str(len(prs))+"   ",end="\r")
    return zeroone(sorted([sorted(rename(cplx,p)) for p in prs])[0],tab)

def cubicalisoreduce(ncplxs,tab,n):
    label_fun = partial(cubicalcanlabel,tab=tab,n=n)
    with Pool() as p:
        labels = p.map(label_fun, ncplxs)
        labels = [l for l in labels]
    #print(time.ctime()+": Done labeling.")
    indices = {}
    for index, number in enumerate(labels):
        if number not in indices:
            indices[number] = index
    indices = list(indices.values())
    #print(time.ctime()+": Done comparing.")
    return [ncplxs[i] for i in indices]

def zeroonesaturatedq(ncplx,tab):
    cplx = fromzeroone(ncplx,tab)
    return min(edgedegs(cplx))>=2

def zeroonevalidq(ncplx,tab):
    cplx = fromzeroone(ncplx,tab)
    return max(edgedegs(cplx))<=2