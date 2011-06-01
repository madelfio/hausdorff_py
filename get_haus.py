import sys
from rtree import Rtree, index

if len(sys.argv) != 3:
    print 'Usage: get_haus <pt_set_file_1> <pt_set_file_2>'
    exit()


p = index.Property()
p.set_index_capacity(4)
p.set_leaf_capacity(10)

def get_points(filename):
    for id, l in enumerate(open(filename)):
        x,y = l.split()
        x,y = float(x), float(y)
        yield (id, (x,y,x,y), None)
        

idx1 = Rtree(get_points(sys.argv[1]), properties=p)
idx2 = Rtree(get_points(sys.argv[2]), properties=p)

idx1.select_mbrs(40);
idx2.select_mbrs(40);

print idx1.mhausdorff(idx2,1)
print idx2.mhausdorff(idx1,1)
print idx1.mhausdorff(idx2,2)
print idx2.mhausdorff(idx1,2)
print idx1.mhausdorff(idx2,0)
print idx2.mhausdorff(idx1,0)
