

import sys
from centroid import Centroid
 import euclidean_distance

def readCentroids():
    centroids = []
    data = [line.rstrip('\n') for line in open('./centroid')]
    for line in data:
        centroid = Centroid()
        centroid.setData(line)
        centroids.append(centroid)
    return centroids

#distance between sets and centroid
def calculateDistance(sets, centroid):
    return euclidean_distance(sets, centroid)

def result():
    centroids = readCentroids()
    miniCenter = 0

    #for each point
    for line in sys.stdin:
        sets = line.split('|')
        Coords = []
        
        for a in sets[1].split('\t'):
            if len(a.strip()) <= 0:
                a = 1
            Coords.append(float(a))
        #calculate distance with the first centroid
        minDist = calculateDistance(Coords, centroids[0].getCurrentCoords())
        
        
        for j in range(1, len(centroids)):
            Dist = calculateDistance(Coords, centroids[j].getCurrentCoords())
            if Dist < miniDist:
                miniCenter = j
                miniDist = Dist
        print('%s,%s' % (miniCenter, line[:-1]))


if __name__ == "__result__":
	result()
