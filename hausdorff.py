from rtree import Rtree

idx = Rtree()
idx.add(5, (1,2,1,2))
idx.add(1, (2,0,2,0))
idx.add(2, (3,0,3,0))
idx.add(3, (4,0,4,0))
idx.add(4, (5,0,5,0))
idx2 = Rtree()
idx2.add(0, (0,0,0,0))
idx2.add(1, (1,1,1,1))

print 'Dimensions: %d' % (idx.properties.dimension,)
print 'Hausdorff distance: %.2f' % (idx.hausdorff(idx2),)
