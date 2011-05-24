import sys
import datetime
from rtree import Rtree
from rtree import index
from array import array
import random

def print_time():
    print datetime.datetime.now()



p = index.Property()
p.set_index_capacity(4)
p.set_leaf_capacity(10)

if len(sys.argv) < 2:
    print "Usage: ne_test.py <query_pt_set_file> <db_pt_set_file_1> <...>"
    exit()

def get_points(filename):
    for id, l in enumerate(open(filename)):
        x,y = l.split()
        x,y = float(x), float(y)
        yield (id, (x,y,x,y), None)
        
        
class Entry:
    def __init__(self, fname, haus, key, pt_set):
        self.fname = fname
        self.haus = haus
        self.key = key
        self.pt_set = pt_set
        

    def __repr__(self):
        return repr((self.fname, self.haus, self.lb1, self.lb2));

class QueryStats:
    def __computeAverage__(self, statList):
        self.numComps = 0
        self.lbTime = 0
        self.totalTime = 0
        for s in statList:
            self.numComps += s.numComps
            self.lbTime += s.lbTime
            self.totalTime += s.totalTime
        self.numComps /= statList.__len__()
        self.lbTime /= statList.__len__()
        self.totalTime /= statList.__len__()    
            
    def __init__(self, numComps=0, lbTime=None, totalTime=None):
        self.numComps = numComps
        if (lbTime is not None):
            self.lbTime = lbTime.seconds*1000.0 + lbTime.microseconds/1000.0
        if (totalTime is not None):
            self.totalTime = totalTime.seconds*1000.0 + lbTime.microseconds/1000.0
            
    def __repr__(self):
        return repr(("NumComps", self.numComps, "lbTime", self.lbTime, "totalTime", self.totalTime));



def SimSearch(queryId, db, lbmode):
    t1 = datetime.datetime.now()
    entryList = list()
    haus = -1
    for (f, pt_set) in db:
        if (not(q_f.__eq__(f))):
            (key, id1, id2) = max(q.hausdorff(pt_set, lbmode), pt_set.hausdorff(q, lbmode))
            entryList.append(Entry(f, 0, key, pt_set))

    t2 = datetime.datetime.now()


    entryList = sorted(entryList, key=lambda Entry: Entry.key)
    min_haus = 100000000
    i = 0

    for e in entryList:
        (e.haus, id1, id2) = max(q.hausdorff(e.pt_set, 0), e.pt_set.hausdorff(q, 0))
        e.haus = e.haus
        if e.haus < min_haus:
               min_haus = e.haus
               nearest_pt_set = e.fname
               minId1 = id1
               minId2 = id2
        i = i + 1
        if (e.key > min_haus): break

    t3 = datetime.datetime.now()
    
    queryRec = QueryStats(i, t2-t1, t3-t1)
    return queryRec

# Loading RTrees        

idx_list = []
for f in sys.argv[1:]:
    try:
        idx = Rtree(get_points(f), properties=p)
    except:
        print "Error reading file: %s" % (f,)
        raise
    idx_list.append((f,idx))

print "Done building DB index"

#random.seed(0)



recList1 = []
recList2 = []

for i in range(20):
    queryid = random.randrange(0, idx_list.__len__(),1)
    (q_f, q) = idx_list[queryid]    

    print "Iteration", i, ": Searching for closest point set to: %s" % (q_f,)

    ### Initial Sorting Using LB  --- Mode = 1
    r1 = SimSearch(queryid, idx_list, 1)
    recList1.append(r1) 
    ### Initial Sorting Using LB' --- Mode = 2
    r2 = SimSearch(queryid, idx_list, 2)
    recList2.append(r2)
    print "Iteration", i, r1
    print "Iteration", i, r2
     
queryRec1 = QueryStats()
queryRec1.__computeAverage__(recList1)

queryRec2 = QueryStats()
queryRec2.__computeAverage__(recList2)

print queryRec1
print queryRec2





























