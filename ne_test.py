import sys
import datetime
from rtree import Rtree
from rtree import index
from array import array

def print_time():
    print datetime.datetime.now()

SCALE = 1000

p = index.Property()
p.set_index_capacity(4)
p.set_leaf_capacity(10)

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
        idx = Rtree(get_points(f), properties=p)
    except:
        print "Error reading file: %s" % (f,)
        raise
    idx_list.append((f,idx))

print "Done building DB index"
#print_time()

(q_f, q) = idx_list[0]
db = idx_list[1:]

print "Searching for closest point set to: %s" % (q_f,)

class Entry:
	def __init__(self, fname, haus, lb1, lb2, pt_set):
		self.fname = fname;
		self.haus = haus
		self.lb1 = lb1;
		self.lb2 = lb2;
		self.pt_set = pt_set;

	def __repr__(self):
		return repr((self.fname, self.haus, self.lb1, self.lb2));

### Initial Sorting Using LB
t1=datetime.datetime.now()

entryList = list()
haus=-1
for (f, pt_set) in db:
    (lb1, id1, id2) = max(q.hausdorff(pt_set,1), pt_set.hausdorff(q,1))
    entryList.append(Entry(f, 0, lb1/SCALE, 0, pt_set))
t2=datetime.datetime.now()


entryList = sorted(entryList, key=lambda Entry: Entry.lb1)
min_haus = 100000000
i=0
for e in entryList:
    	(e.haus, id1, id2) = max(q.hausdorff(pt_set,0), e.pt_set.hausdorff(q,0))
	e.haus = e.haus/SCALE
	min_haus = min(e.haus,min_haus)
	i=i+1
	if (e.lb1 > min_haus): break
	#print "%f *%f (%f)" % (e.haus, e.lb1,  e.lb1/e.haus)
print i

t3=datetime.datetime.now()
diff1=t2-t1
diff2=t3-t1
print diff1, diff2

### Initial Sorting Using LB'
t1=datetime.datetime.now()
entryList = list()
for (f, pt_set) in db:
    (lb2, id1, id2) = max(q.hausdorff(pt_set,2), pt_set.hausdorff(q,2))
    entryList.append(Entry(f, 0, 0, lb2/SCALE, pt_set))

t2=datetime.datetime.now()

entryList = sorted(entryList, key=lambda Entry: Entry.lb2)
min_haus = 1000000000
i=0
for e in entryList:
    	(e.haus, id1, id2) = max(q.hausdorff(pt_set,0), e.pt_set.hausdorff(q,0))
	e.haus = e.haus/SCALE
   	if e.haus < min_haus:
        	min_haus = e.haus
        	nearest_pt_set = e.fname
	i=i+1
	if (e.lb2 > min_haus): break
	r = (e.lb2-e.lb1)/(e.haus-e.lb1)
	#print "%f *%f (%f)" % (e.haus, e.lb2, e.lb2/e.haus)
print i

t3=datetime.datetime.now()
diff1=t2-t1
diff2=t3-t1
print diff1, diff2



print "Results:"
print "  Nearest Point Set: %s" % nearest_pt_set
print "  Hausdorff Distance: %f" % (min_haus, )
#print_time()
