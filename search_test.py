import datetime
import time
from rtree import Rtree, index
import heapq
import random

#######################################
# Accept and parse command line options
from optparse import OptionParser
usage = "Usage: %prog [options] query_pt_set_files"
parser = OptionParser(usage=usage)
parser.add_option("-m", "--mhaus", action="store_true", dest="mhaus",
                  default=False)
parser.add_option("-n", "--num_queries", type="int", dest="num_queries")
parser.add_option("-k", type="int", dest="k")
parser.add_option("--rand-seed", type="int", dest="rand_seed")
parser.add_option("--pairwise", action="store_true", dest="pairwise",
                  default=True)
parser.add_option("--nn-scan", action="store_false", dest="pairwise")

parser.add_option("--min-mbrs", type="int", dest="min_mbrs", default=20)
parser.add_option("--max-mbrs", type="int", dest="max_mbrs")
parser.add_option("--mbr-step", type="int", dest="mbr_step", default=20)

parser.add_option("--min-k", type="int", dest="min_k", default=20)
parser.add_option("--max-k", type="int", dest="max_k")
parser.add_option("--k-step", type="int", dest="k_step", default=20)

parser.add_option("-x", "--experiment-number", type="int", dest="exp_num",
                  default=1)
(options, args) = parser.parse_args()

if len(args) < 1:
    parser.error("query_pt_set_files are missing")
    exit()

MHAUS = options.mhaus
NUM_QUERIES = options.num_queries or 10
DEFAULT_K = options.k or 2
RAND_SEED = options.rand_seed or hash(datetime.datetime.now()) % 10000

MIN_MBRS, MAX_MBRS, MBR_STEP = (options.min_mbrs, 
                                options.max_mbrs,
                                options.mbr_step)

MIN_K, MAX_K, K_STEP = (options.min_k, options.max_k, options.k_step) 

EXP_NUM = options.exp_num

if options.pairwise:
    HAUSDORFF_MODE = 0
else:
    HAUSDORFF_MODE = -1

DEFAULT_MBR_CNT = MIN_MBRS

#######################################

p = index.Property()
p.set_index_capacity(4)
p.set_leaf_capacity(10)

index_list = []

def print_time():
    print datetime.datetime.now()

def get_points(filename):
    for id, l in enumerate(open(filename)):
        x,y = l.split()
        x,y = 1000*float(x), 1000*float(y)
        yield (id, (x,y,x,y), None)
        
        
class Entry:
    def __init__(self, fname, haus, key, pt_set_id, stage=1):
        self.fname = fname
        self.haus = haus
        self.key = key
        self.pt_set_id = pt_set_id
        self.stage = stage
        
    def __repr__(self):
        return repr((self.fname, self.haus, self.key, self.stage));

class QueryStats:
    def __computeAverage__(self, statList):
        self.numComps = 0
        self.lbTime = 0
        self.hausTime = 0
        self.totalTime = 0
        self.numIterations = 0
        self.traversalCost = 0
        self.num_dist_cals = 0
        for s in statList:
            self.numComps += s.numComps
            self.lbTime += s.lbTime
            self.hausTime += s.hausTime
            self.totalTime += s.totalTime
            self.numIterations += s.numIterations
            self.traversalCost += s.traversalCost
            self.num_dist_cals += s.num_dist_cals
            
        self.numComps /= len(statList)
        self.lbTime /= len(statList)
        self.hausTime /= len(statList)
        self.totalTime /= len(statList)
        self.numIterations /= len(statList)
        self.traversalCost /= len(statList)
        self.num_dist_cals /= len(statList)

    def __init__(self, numIterations=0, numComps=0, lbTime=None, hausTime=None,
                 totalTime=None, traversalCost=None, num_dist_cals=None):
        self.numComps = numComps
        self.numIterations = numIterations
        self.lbTime=lbTime
        self.hausTime=hausTime
        self.totalTime=totalTime
        self.traversalCost=traversalCost
        self.num_dist_cals=num_dist_cals
            
    def __repr__(self):
        return ', '.join(("NumIterations: %4d" % (self.numIterations,),
                          "NumComps: %4d" % (self.numComps,),
                          "lbTime: %8.3f" % (self.lbTime,),
                          "hausTime: %9.3f" % (self.hausTime,),
                          "totalTime: %9.3f" % (self.totalTime,),
                          "traversalCost: %d" % (self.traversalCost,),
                          "num_dist_cals: %9d" % (self.num_dist_cals,)
                         ))

def floateq(a,b):
    if a == b == 0:
        return True
    val = (a-b)/b
    threshold = 0.0001
    if (val > threshold or val < -threshold): return False
    return True

def CheckResults(list1, list2, list3):
    for i in range(len(list1)):
        if (not floateq(list1[i].haus,list2[i].haus)):
            return False
        if (not floateq(list1[i].haus,list3[i].haus)):
            return False
    return True


def compute_hausdorff_by_id(id1, id2, lb_mode):
    if MHAUS:
        return index_list[id1][1].mhausdorff(index_list[id2][1], lb_mode)
    else:
        return index_list[id1][1].hausdorff(index_list[id2][1], lb_mode)


def init_priority_queue(query_id, lb_mode):
    pq = []
    lb_elap = 0
    num_dist_cals = 0
    for i in range(len(index_list)):
        if i != query_id:
            (key, info) = compute_hausdorff_by_id(query_id, i, lb_mode)
            heapq.heappush(pq, (key, i, Entry(index_list[i][0], 0, key, i)))
            num_dist_cals += info.num_dist_cals
            lb_elap += info.elap
    return (pq, lb_elap, num_dist_cals)


def SimSearch(query_id, lbmode, k=1, inc=False):
 
    start_time = datetime.datetime.now()
    (pq, lb_time, num_dist_cals) = init_priority_queue(query_id, lbmode)

    if lbmode == -1:
        return

    resultList = []

    numComps = 0
    numIterations = 0
    traversalCost = 0
    haus_time = 0
    while (len(pq) > 0 and len(resultList) < k):
        numIterations += 1
        (key, id, e) = heapq.heappop(pq)

        if (e.stage == 1 and inc):
            #ta = datetime.datetime.now()
            (e.key, info) = compute_hausdorff_by_id(query_id, e.pt_set_id, 2)
            #tb = datetime.datetime.now()
            lb_time = lb_time + info.elap #(tb-ta)
            e.stage = 2
            num_dist_cals += info.num_dist_cals
            heapq.heappush(pq, (e.key, e.pt_set_id, e))
        elif (e.stage == 3):
            resultList.append(e)
        else:
            (e.haus, info) = compute_hausdorff_by_id(query_id, e.pt_set_id,
                                                     HAUSDORFF_MODE)
            numComps += 1
            haus_time += info.elap
            e.stage = 3
            traversalCost += info.traversal_cost
            num_dist_cals += info.num_dist_cals
            heapq.heappush(pq, (e.haus, e.pt_set_id, e))

    end_time = datetime.datetime.now()
    total_time = end_time - start_time
    total_time = total_time.seconds * 1000.0 + total_time.microseconds / 1000.0
    
    queryRec = QueryStats(numIterations, numComps, lb_time, haus_time, total_time,
                          traversalCost, num_dist_cals)
    return (queryRec,resultList)


def RunExperiments(queryList, the_k_value):

    recList1 = []
    recList2 = []
    recList3 = []

    for queryid in queryList:  

        print "k value:", the_k_value, 
        print ": Searching for closest point set to: %s" % (index_list[queryid][0],)


        ### Test run to "warm up" the caches
        #SimSearch(queryid, -2, the_k_value)

        ### Initial Sorting Using LB  --- Mode = 1
        (rec1, res1) = SimSearch(queryid, 1, the_k_value)
        #print rec1
        recList1.append(rec1) 
    
        ### Initial Sorting Using LB' --- Mode = 2
        (rec2, res2) = SimSearch(queryid, 2, the_k_value)
        #print rec2
        recList2.append(rec2)
    
        ### Initial Sorting Using LB --- Mode = 1
        ### Re-sorting Using LB' --- Incremental
        (rec3, res3) = SimSearch(queryid, 1, the_k_value, True)
        #print rec3
        recList3.append(rec3)
    
        if (not CheckResults(res1,res2,res3)):
            print "Result Mismatch!"
            break;

        time.sleep(0.1)
    
    queryRec1 = QueryStats()
    queryRec1.__computeAverage__(recList1)

    queryRec2 = QueryStats()
    queryRec2.__computeAverage__(recList2)

    queryRec3 = QueryStats()
    queryRec3.__computeAverage__(recList3)

    return (queryRec1, queryRec2, queryRec3)

def select_all_mbrs(mbr_cnt):
    for (f, idx) in index_list:
        idx.select_mbrs(mbr_cnt)

def experiment1_mbr_count(queryList):
    for mbr_cnt in range(MIN_MBRS, MAX_MBRS + 1, MBR_STEP):
        print 'testing with %d MBRs' % (mbr_cnt,)
        select_all_mbrs(mbr_cnt)
        (rec1, rec2, rec3) = RunExperiments(queryList, DEFAULT_K)
        print rec1
        print rec2
        print rec3
        print

def experiment2_k_Values(queryList):
    select_all_mbrs(DEFAULT_MBR_CNT)
    for k in range(MIN_K, MAX_K + 1, K_STEP):
        (rec1,rec2,rec3) = RunExperiments(queryList, k)
        print "k", k, rec1
        print "k", k, rec2
        print "k", k, rec3

def experiment3_enh_lb_stats(queryPairs):
    select_all_mbrs(MIN_MBRS)
    print 'Using %d MBRs' % (MIN_MBRS,)
    for (idx1, idx2) in queryPairs:
        print 'From %s to %s:' % (index_list[idx1][0],
                                  index_list[idx2][0])
        (lb, elb, haus) = (compute_hausdorff_by_id(idx1, idx2, mode)
                           for mode in [1,2,0])
        print '%10.6f, %10.6f, %10.6f' % (lb[0]/1000.0, elb[0]/1000.0,
                                          haus[0]/1000.0)
        print '  (LB:   %r)' % (lb,)
        print '  (ELB:  %r)' % (elb,)
        print '  (HAUS: %r)' % (haus,)

def experiment4_enh_lb_stats(query_list):
    select_all_mbrs(MIN_MBRS)
    print 'Using %d MBRs' % (MIN_MBRS,)

    # for each pointset in index_list:
    #   compute lb, elb, haus
    # sort list by lb/elb/haus(?)
    # print sorted list with ranks

    full_results = []
    for query_id in query_list:
        results = []
        for i in range(len(index_list)):
            if i != query_id:
                (lb, info) = compute_hausdorff_by_id(query_id, i, 1)
                (elb, info) = compute_hausdorff_by_id(query_id, i, 2)
                (haus, info) = compute_hausdorff_by_id(query_id, i, 0)
                #results.append((lb/1000.0, elb/1000.0, haus/1000.0))
                if haus == 0:
                    #results.append((0.0,0.0,1.0))
                    results.append((haus,0.0,0.0))
                else:
                    #results.append((lb/haus, elb/haus, 1.0))
                    results.append((haus, lb/haus, elb/haus))

        results.sort()
        full_results.append(results)

    r = [(0,0,0)] * (len(index_list) - 1)
    for i in range(len(query_list)):
        for j in range(len(index_list) - 1):
            a = r[j]
            b = full_results[i][j]
            r[j] = (a[0] + b[0], a[1] + b[1], a[2] + b[2])

    x = len(full_results)
    r = [(a/x, b/x, c/x) for (a,b,c) in r]

    print '\n'.join("%d, %10.6f, %10.6f, %10.6f" % (i, lb, elb, haus) 
                    for (i, (lb, elb, haus)) in enumerate(r))
        

###
### Main Function.... well.... more precisely, the code fragment which controls this script
###

# Loading RTrees        

def main():
    print "Indexing %d pointsets" % (len(args) - 1,)
    for f in args:
        try:
            idx = Rtree(get_points(f), properties=p)
        except:
            print "Error reading file: %s" % (f,)
            raise
        index_list.append((f,idx))

    print "Done building DB index"

    min_k = 2
    max_k = 3

    num_queries = NUM_QUERIES #len(index_list)
    queryList = []

    randseed = RAND_SEED


    print "randseed = ", randseed

    random.seed(randseed)

    queryList = random.sample(range(len(index_list)), num_queries)

    if EXP_NUM == 1:
        experiment1_mbr_count(queryList)
    elif EXP_NUM == 2:
        experiment2_k_Values(queryList)
    elif EXP_NUM == 3:
        queryPairs = random.sample(range(len(index_list)), num_queries)
        queryPairs = zip(queryPairs[::2], queryPairs[1::2])
        experiment3_enh_lb_stats(queryPairs)
    elif EXP_NUM == 4:
        query_list = random.sample(range(len(index_list)), num_queries)
        experiment4_enh_lb_stats(query_list)

if __name__ == '__main__':
    main()
