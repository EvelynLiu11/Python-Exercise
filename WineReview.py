# -*- coding: utf-8 -*-
"""
Created on Sun Feb  3 20:29:23 2019

@author: Liu
"""

from mrjob.job import MRJob
from mrjob.step import MRStep


class WineReview(MRJob):
    
    def steps(self):
        return [
                MRStep(mapper=self.mapper_0,
                       reducer=self.reducer_0),
                MRStep(mapper=self.mapper_1,
                       reducer=self.reducer_1)
        ]

    def mapper_0(self, _, line):
        (country, designation, province, region_1, price, point) = line.split(',')
        if float(price) != 0 and int(point) != 0:
            yield (country, designation), (float(price), int(point))

    def reducer_0(self, position, values):
        prices = []
        points = []
        for price, point in values:
            prices.append(price)
            points.append(point)
        price_avg = sum(prices) / len(prices)
        point_avg = sum(points) / len(points)
        yield position, (price_avg, point_avg)

    def mapper_1(self, position, values):
        country, designation = position
        price, point = values
        yield '%08.02f'%float(point), (country, designation, price)
    
    def reducer_1(self, point, values): 
        for country, designation, price in values:
            yield (country, designation), (float(price), float(point))
        
if __name__ == '__main__':
    WineReview.run()
    
# !python WineReview.py modifiedWine.csv > WineInfo.txt
