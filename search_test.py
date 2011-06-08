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

if options.pairwise:
    HAUSDORFF_MODE = 0
else:
    HAUSDORFF_MODE = -1

DEFAULT_MBR_CNT = 40

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


def compute_dists (stat_list_lb, stat_list_elb):
    ratios = []
    for (stat_lb, stat_elb) in zip(stat_list_lb, stat_list_elb):
        ratios.append(((1.0 * stat_elb.numIterations /
                        stat_lb.numIterations),
                       (1.0 * stat_elb.numComps / stat_lb.numComps),
                       (stat_elb.lbTime / stat_lb.lbTime +
                        0.000000000001),
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

    scale = 200.0 / len(ratios) #(200 "X"s to represent the values)
    keys = comp_hist.keys()
    for bin in range(int(min(keys)), int(max(keys))):
        print '%.3f: %s' % (bin_interval * bin,
                            int(comp_hist.get(bin,0.0) * scale) * "X")
    print

    #keys = lb_time_hist.keys()
    #for bin in range(int(min(keys)), int(max(keys))):
    #    print '%.3f: %s' % (bin_interval * bin,
    #                        int(lb_time_hist.get(bin,0.0) * scale) * "X")
    #print

    keys = total_time_hist.keys()
    for bin in range(int(min(keys)), int(max(keys))):
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
 
    t1 = datetime.datetime.now()
    (pq, lb_time, num_dist_cals) = init_priority_queue(query_id, lbmode)
    #t2 = datetime.datetime.now()
    #lbtime = t2 - t1

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

    t3 = datetime.datetime.now()
    total_time = t3-t1
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

    #compute_dists(recList1, recList3)

    return (queryRec1, queryRec2, queryRec3)

def Experiment1_k_Values(queryList, min_k, max_k):
    select_all_mbrs(DEFAULT_MBR_CNT)
    for k in range(min_k, max_k, min_k):
        (rec1,rec2,rec3) = RunExperiments(queryList, k)
        print "k", k, rec1
        print "k", k, rec2
        print "k", k, rec3

def select_all_mbrs(mbr_cnt):
    for (f, idx) in index_list:
        idx.select_mbrs(mbr_cnt)

def experiment2_mbr_count(queryList):
    for mbr_cnt in range(MIN_MBRS, MAX_MBRS + 1, MBR_STEP):
        print 'testing with %d MBRs' % (mbr_cnt,)
        select_all_mbrs(mbr_cnt)
        (rec1, rec2, rec3) = RunExperiments(queryList, DEFAULT_K)
        print rec1
        print rec2
        print rec3
        print

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

    #Experiment1_k_Values(queryList, min_k, max_k)
    experiment2_mbr_count(queryList)

if __name__ == '__main__':
    main()
