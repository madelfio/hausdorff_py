from rtree import Rtree

idx = Rtree()
idx.add(5, (1,2,1,2))
idx.add(1, (2,0,2,0))
idx.add(2, (3,0,3,0))
idx.add(3, (4,0,4,0))
idx.add(4, (5,0,5,0))
idx2 = Rtree()
idx2.add(10, (0,0,0,0))
idx2.add(11, (1,1,1,1))

print 'Dimensions: %d' % (idx.properties.dimension,)
h = idx.hausdorff(idx2)
id1 = h[1].contents.value
id2 = h[2].contents.value
print 'Hausdorff distance: %.2f, %d, %d' % (h[0], id1, id2)
