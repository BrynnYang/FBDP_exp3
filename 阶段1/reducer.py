# coding=UTF-8
# !/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys

key={}

# input comes from STDIN
for row in sys.stdin:
    row = row.strip()
    # from map.py
    province, value = row.split('\t')
    item_id, action = value.split(',')
    sales=int(action)
    if province not in key:
        key[province]=[{},{}]
        commodity_top10_1=key[province][0]
        commodity_top10_2=key[province][1]
        commodity_top10_1[item_id]=commodity_top10_1.get(item_id,0)+1
        if sales == 2:
            commodity_top10_2[item_id]=commodity_top10_2.get(item_id,0)+1
    else:
        commodity_top10_1=key[province][0]
        commodity_top10_2=key[province][1]
        commodity_top10_1[item_id]=commodity_top10_1.get(item_id,0)+1
        if sales == 2:
            commodity_top10_2[item_id]=commodity_top10_2.get(item_id,0)+1

for province in key:
    commodity_top10_1=key[province][0]
    commodity_top10_2=key[province][1]
    sort1 = sorted(commodity_top10_1.items(),key=lambda x: x[1], reverse=True)
    sort2 = sorted(commodity_top10_2.items(),key=lambda x: x[1], reverse=True)
    print(province+':'+'\n')
    print('前十热门关注产品:')
    for i in range(10):
        print(sort1[i], end=" ")
    print('\n')
    print('前十热门销售产品:')
    for k in range(10):
        print(sort2[k], end=" ")
    print('\n')
