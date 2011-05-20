import sys
import datetime
from rtree import Rtree

def print_time():
    print datetime.datetime.now()

SCALE = 1000

if len(sys.argv) < 2:
    print "Usage: ne_test.py <query_pt_set_file> <db_pt_set_file_1> <...>"
    exit()

def get_points(filename):
    for id, l in enumerate(open(filename)):
        x,y = l.split()
        x,y = float(x)*SCALE, float(y)*SCALE
        yield (id, (x,y,x,y), None)

idx_list = []
for f in sys.argv[1:]:
    try:
        idx = Rtree(get_points(f))
    except:
        print "Error reading file: %s" % (f,)
        raise
    idx_list.append((f,idx))

print "Done building DB index"
#print_time()

(q_f, q) = idx_list[0]
db = idx_list[1:]

print "Searching for closest point set to: %s" % (q_f,)

min_haus = 0 
nearest_pt_set = -1
for (f, pt_set) in db:
    haus = max(q.hausdorff(pt_set), pt_set.hausdorff(q))
    #print "%s: %f" % (f, haus)
    if haus > min_haus:
        min_haus = haus
        nearest_pt_set = f

print "Results:"
print "  Nearest Point Set: %s" % nearest_pt_set
print "  Hausdorff Distance: %f" % (min_haus / SCALE, )
#print_time()
