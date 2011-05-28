import sys
import datetime
from rtree import Rtree
from rtree import index
from array import array
import heapq
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
        x,y = 1000*float(x), 1000*float(y)
        yield (id, (x,y,x,y), None)
        
        
class Entry:
    def __init__(self, fname, haus, key, pt_set, stage=1):
        self.fname = fname
        self.haus = haus
        self.key = key
        self.pt_set = pt_set
        self.stage = stage
        
    def __repr__(self):
        return repr((self.fname, self.haus, self.key, self.stage));

class QueryStats:
    def __computeAverage__(self, statList):
        self.numComps = 0
        self.lbTime = 0
        self.totalTime = 0
        self.numIterations = 0
        for s in statList:
            self.numComps += s.numComps
            self.lbTime += s.lbTime
            self.totalTime += s.totalTime
            self.numIterations += s.numIterations
            
        self.numComps /= statList.__len__()
        self.lbTime /= statList.__len__()
        self.totalTime /= statList.__len__()    
        self.numIterations /= statList.__len__()    

    def __init__(self, numIterations=0, numComps=0, lbTime=None, totalTime=None):
        self.numComps = numComps
        self.numIterations = numIterations
        if (lbTime is not None):
            self.lbTime = lbTime.seconds*1000.0 + lbTime.microseconds/1000.0
        if (totalTime is not None):
            self.totalTime = totalTime.seconds*1000.0 + totalTime.microseconds/1000.0
            
    def __repr__(self):
        return ', '.join(("NumIterations: %3d" % (self.numIterations,),
                          "NumComps: %3d" % (self.numComps,),
                          "lbTime: %8.3f" % (self.lbTime,),
                          "totalTime: %8.3f" % (self.totalTime,)))

def compute_dists (stat_list_lb, stat_list_elb):
    ratios = []
    for (stat_lb, stat_elb) in zip(stat_list_lb, stat_list_elb):
        ratios.append(((1.0 * stat_elb.numIterations /
                        stat_lb.numIterations),
                       (1.0 * stat_elb.numComps / stat_lb.numComps),
                       (stat_elb.lbTime / stat_lb.lbTime),
                       (stat_elb.totalTime / stat_lb.totalTime)))

    # put ratios into bins
    iter_hist = {}
    comp_hist = {}
    lb_time_hist = {}
    total_time_hist = {}
    bin_interval = 0.025

    for iter, comp, lb_time, total_time in ratios:
        iter_bin = round(iter/bin_interval)
        comp_bin = round(comp/bin_interval)
        lb_time_bin = round(lb_time/bin_interval)
        total_time_bin = round(total_time/bin_interval)

        iter_hist[iter_bin] = iter_hist.get(iter_bin, 0) + 1
        comp_hist[comp_bin] = comp_hist.get(comp_bin, 0) + 1
        lb_time_hist[lb_time_bin] = lb_time_hist.get(lb_time_bin, 0) + 1
        total_time_hist[total_time_bin] = total_time_hist.get(total_time_bin, 0) + 1

    scale = 200.0 / len(ratios) #(200 "X"s to represent the values
    keys = comp_hist.keys()
    for bin in range(min(keys), max(keys)):
        print '%.3f: %s' % (bin_interval * bin,
                            int(comp_hist.get(bin,0.0) * scale) * "X")
    print

    keys = lb_time_hist.keys()
    for bin in range(min(keys), max(keys)):
        print '%.3f: %s' % (bin_interval * bin,
                            int(lb_time_hist.get(bin,0.0) * scale) * "X")
    print

    keys = total_time_hist.keys()
    for bin in range(min(keys), max(keys)):
        print '%.3f: %s' % (bin_interval * bin,
                            int(total_time_hist.get(bin,0.0) * scale) * "X")
    print

        


def floateq(a,b):
    if a == b == 0:
        return True
    val = (a-b)/b
    threshold = 0.0001
    if (val > threshold or val < -threshold): return False
    return True

def CheckResults(list1, list2, list3):
    for i in range(list1.__len__()):
        if (not floateq(list1[i].haus,list2[i].haus)):
            return False
        if (not floateq(list1[i].haus,list3[i].haus)):
            return False
    return True


def SimSearch(queryId, db, lbmode, k=1, inc=False):
  
    t1 = datetime.datetime.now()
    (q_f, q) = idx_list[queryId] 
    resultList = list()
    pq = []
    haus = -1
    for (f, pt_set) in db:
        if (not(q_f.__eq__(f))):
            (key, id1, id2) = max(q.hausdorff(pt_set, lbmode), pt_set.hausdorff(q, lbmode))
            heapq.heappush(pq, (key, Entry(f, 0, key, pt_set)))

    t2 = datetime.datetime.now()

    lbtime = t2 - t1

    if lbmode == -1:
        return

    #entryList = sorted(entryList, key=lambda Entry: Entry.key)
    min_haus = 100000000
    numComps = 0
    count = 0
    numIterations = 0
    while pq.__len__() > 0:
        numIterations = numIterations + 1
        (key, e) = heapq.heappop(pq)
        if (e.stage == 1 and inc):
            ta = datetime.datetime.now()
            (e.key, id1, id2) = max(q.hausdorff(e.pt_set, 2), e.pt_set.hausdorff(q, 2))
            tb = datetime.datetime.now()
            lbtime = lbtime + (tb-ta)
            e.stage = 2
            heapq.heappush(pq, (e.key, e))
        elif (e.stage == 3):
            count = count+1
            resultList.append(e)
            if (count == k): break
        else:
            (e.haus, id1, id2) = max(q.hausdorff(e.pt_set, 0), e.pt_set.hausdorff(q, 0))
            numComps = numComps +1
            e.stage = 3
        
            next_key = 100000000
            if (len(pq) > 0):
                (next_key, next_e) = pq[0]
  
            if (e.haus < next_key):
                count = count+1
                resultList.append(e)
                if (count == k): break
            else:
                heapq.heappush(pq, (e.haus, e))

    t3 = datetime.datetime.now()
    
    queryRec = QueryStats(numIterations, numComps, lbtime, t3-t1)
    return (queryRec,resultList)



#random.seed(0)


def RunExperiments(queryList, the_k_value, idx_list):

    recList1 = []
    recList2 = []
    recList3 = []

    for queryid in queryList:  

        print "k value:", the_k_value, 
        print ": Searching for closest point set to: %s" % (idx_list[queryid][0],)


        ### Initial Sorting Using LB  --- Mode = 1
        SimSearch(queryid, idx_list, -1, the_k_value)

        ### Initial Sorting Using LB  --- Mode = 1
        (rec1, res1) = SimSearch(queryid, idx_list, 1, the_k_value)
        print rec1
        recList1.append(rec1) 
    
        ### Initial Sorting Using LB' --- Mode = 2
        (rec2, res2) = SimSearch(queryid, idx_list, 2, the_k_value)
        print rec2
        recList2.append(rec2)
    
        ### Initial Sorting Using LB --- Mode = 1
        ### Re-sorting Using LB' --- Incremental
        (rec3, res3) = SimSearch(queryid, idx_list, 1, the_k_value, True)
        print rec3
        recList3.append(rec3)
    
        if (not CheckResults(res1,res2,res3)):
            print "Result Mismatch!"
            break;
    
        #print "Iteration", i, "k", the_k_value, rec1
        #print "Iteration", i, "k", the_k_value, rec2
        #print "Iteration", i, "k", the_k_value, rec3
  
    queryRec1 = QueryStats()
    queryRec1.__computeAverage__(recList1)

    queryRec2 = QueryStats()
    queryRec2.__computeAverage__(recList2)

    queryRec3 = QueryStats()
    queryRec3.__computeAverage__(recList3)

    compute_dists(recList1, recList3)

    return (queryRec1, queryRec2, queryRec3)

def Experiment1_k_Values(queryList, idx_list, min_k, max_k):
    for k in range(min_k, max_k, min_k):
        (rec1,rec2,rec3) = RunExperiments(queryList, k, idx_list)
        print "k", k, rec1
        print "k", k, rec2
        print "k", k, rec3

###
### Main Function.... well.... more precisely, the code fragment which controls this script
###

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

min_k = 2
max_k = 3

num_runs = 200 #len(idx_list)
queryList = []

randseed = hash(datetime.datetime.now())
print "randseed = ", randseed


random.seed(randseed)

queryList = random.sample(range(len(idx_list)), num_runs)
#for i in range(num_runs):
#    queryid = random.randrange(0, len(idx_list), 1)
#    queryList.append(queryid)


Experiment1_k_Values(queryList, idx_list, min_k, max_k)
