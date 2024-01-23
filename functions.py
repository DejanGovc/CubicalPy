from functools import lru_cache
from itertools import product
from itertools import groupby
from itertools import permutations

### FORMATS FOR SQLITE3:

def int_to_blob(val,n2):
    num_bytes = n2//8  #8 bits per byte
    return val.to_bytes(num_bytes, byteorder='big', signed=False)

def blob_to_int(val):
    return int.from_bytes(val, byteorder='big', signed=False)

### PERMUTATIONS:

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

### DIFFERENT ENCODINGS OF COMPLEXES:

def zeroone(lst,tab):
    zo = 0
    for e in lst:
        zo |= (1 << tab.index(e))
    return zo

def fromzeroone(zo,tab):
    fromzo = []
    lst = [int(bit) for bit in bin(zo)[2:]]
    lst.reverse()
    lst += [0 for _ in range(len(tab)-len(lst))]
    for i in range(len(lst)):
        if lst[i] == 1:
            fromzo.append(tab[i])
    return(sorted(fromzo))

### FACES, BOUNDARIES, DEGREES:

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

@lru_cache(maxsize=None)
def boundaries(n):
    return tuple(zeroone(bdry(c),cubes(n,1)) for c in cubes(n,2))

@lru_cache(maxsize=None)
def bboundaries(n):
    return tuple(zeroone(bbdry(c),cubes(n,0)) for c in cubes(n,2))

def testedges(ncplx,n):
    n2 = n*(n-1)*2**(n-3)
    once = 0
    twice = 0
    for i in range(n2):
        if ncplx & (1 << i):
            twice |= once & boundaries(n)[i]
            once ^= boundaries(n)[i]
            if once & twice:
                return (False,False)
    return (True,once==0,once)

def alledges(ncplx,n):
    n2 = n*(n-1)*2**(n-3)
    edges = 0
    for i in range(n2):
        if ncplx & (1 << i):
            edges |= boundaries(n)[i]
    return edges

def firstone(n):
    if n == 0:
        raise ValueError("Zero does not have a first nonzero bit.")
    index = 0
    while True:
        if (n >> index) & 1:
            return index
        else:
            index += 1

@lru_cache(maxsize=None)
def edgesquares(k,n):
    return zeroone([c for c in cubes(n,2) if cubes(n,1)[k] in bdry(c)],cubes(n,2))

### AUTOMORPHISMS AND CANONICAL LABELING (assuming testedges(ncplx,n)[0]==True and n <= 7):

@lru_cache(maxsize=None)
def edgeboundaries(n):
    return tuple(zeroone(bdry(c),cubes(n,0)) for c in cubes(n,1))

def fake_octal(x):
    return int(bin(x)[2:],8)

def testvertices(ncplx,n):
    n2 = n*(n-1)*2**(n-3)
    n1 = n*2**(n-1)
    n0 = 2**n
    edges = alledges(ncplx,n)
    vine = 0
    vins = 0
    for i in range(n1):
        if edges & (1 << i):
            vine += fake_octal(edgeboundaries(n)[i])
    for i in range(n2):
        if ncplx & (1 << i):
            vins += fake_octal(bboundaries(n)[i])
    tupvine = tuple(int(i) for i in oct(vine)[2:])
    tupvine = tuple(0 for _ in range(n0-len(tupvine))) + tupvine
    tupvins = tuple(int(i) for i in oct(vins)[2:])
    tupvins = tuple(0 for _ in range(n0-len(tupvins))) + tupvins
    return tuple(reversed(tuple(zip(tupvine,tupvins))))

@lru_cache(maxsize=None)
def nbhd(v):
    return tuple(v[:i]+(1-v[i],)+v[i+1:] for i in range(len(v)))

@lru_cache(maxsize=None)
def nnbhd(i,n):
    v = cubes(n,0)[i]
    return tuple([cubes(n,0).index(u) for u in nbhd(v)])

def bigdegs(ncplx,n):
    dd = testvertices(ncplx,n)
    return tuple([dd[v]+tuple(i for t in sorted([dd[u] for u in nnbhd(v,n)]) for i in t) for v in range(1<<n)])

def bigbigdegs(ncplx,n):
    dd = bigdegs(ncplx,n)
    return tuple([dd[v]+tuple(i for t in sorted([dd[u] for u in nnbhd(v,n)]) for i in t) for v in range(1<<n)])

def pairs(dd,n):
    mindeg = min(dd)
    minverts = [v for v in range(1<<n) if dd[v] == mindeg]
    pp = []
    for v in minverts:
        nbdegs = [dd[u] for u in nnbhd(v,n)]
        pi = sorting_perm(nbdegs)
        permlist = [tuple(pi[i] for i in tau) for tau in perms(repeats(nbdegs))]
        pp += [(cubes(n,0)[v],tau) for tau in permlist]
    return tuple(pp)

@lru_cache(maxsize=None)
def renamecube(c,p):
    newc = tuple(1-c[i] if p[0][i] == 1 and c[i] <= 1 else c[i] for i in range(len(c)))
    return tuple(newc[i] for i in p[1])

def rename(cplx,p):
    return [renamecube(c,p) for c in cplx]

@lru_cache(maxsize=None)
def faceperm(p,n):
    squareindices = {c:index for index,c in enumerate(cubes(n,2))}
    return tuple(squareindices[renamecube(c,p)] for c in cubes(n,2))

def apply(perm,ncplx,n):
    permcplx = 0
    n2 = n*(n-1)*2**(n-3)
    for i in range(n2):
        if ncplx & (1 << i):
            permcplx |= (1 << perm[i])
    return permcplx

def cubicalcanlabel(ncplx,n):
    dd = bigbigdegs(ncplx,n)
    prs = pairs(dd,n)
    return sorted([apply(faceperm(p,n),ncplx,n) for p in prs])[0]

### EXTENSION (FOR VARIOUS TYPES OF SURFACES):

def extendonce(ncplx,n):
    ncplxsnew = []
    n2 = n*(n-1)*2**(n-3)
    es = edgesquares(firstone(testedges(ncplx,n)[2]),n)
    for i in range(n2):
        if es & (1 << i) and not ncplx & (1 << i):
            ncplxsnew.append(ncplx|(1<<i))
    return ncplxsnew

def disconnected_withbdry_extendonce(ncplx,n):
    ncplxsnew = []
    n2 = n*(n-1)*2**(n-3)
    for i in range(n2):
        if not ncplx & (1 << i):
            ncplxsnew.append(ncplx|(1<<i))
    return ncplxsnew

def withbdry_extendonce(ncplx,n):
    ncplxsnew = []
    n1 = n*2**(n-1)
    n2 = n*(n-1)*2**(n-3)
    es = 0
    t = testedges(ncplx,n)[2]
    for i in range(n1):
        if (t >> i) & 1:
            es |= edgesquares(i,n)
    for i in range(n2):
        if es & (1 << i) and not ncplx & (1 << i):
            ncplxsnew.append(ncplx|(1<<i))
    return ncplxsnew

def disconnected_extendonce(ncplx,n):
    ncplxsnew = []
    n2 = n*(n-1)*2**(n-3)
    t = testedges(ncplx,n)[2]
    if t == 0:
        for i in range(n2):
            if not ncplx & (1 << i):
                ncplxsnew.append(ncplx|(1<<i))
        return ncplxsnew
    es = edgesquares(firstone(t),n)
    for i in range(n2):
        if es & (1 << i) and not ncplx & (1 << i):
            ncplxsnew.append(ncplx|(1<<i))
    return ncplxsnew