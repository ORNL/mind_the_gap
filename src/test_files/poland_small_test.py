"""Tests auto gap minding on a small piece of poland for faster debugging"""

import geopandas as gpd

import progress

sample = progress.country('poland',
                          'postgresql://openadmin:openadmin@manhattan:3022/opendb',
                          bound_path='./sample_bound.geojson',
                          build_path='./sample.geojson',
                          bound_from_file=True,
                          build_from_file=True)

#sample.mind(0.015,2,2,20)
#print('minded')
#sample.make_grid(size=0.01)
#sample.fit_check(1,1)

#poland.fit_check(1,1)

#poland.gaps.to_file('./poland_test_gaps.geojson', driver='GeoJSON')

sample.prog()
print(sample.gaps)

sample.gaps.to_file('samplegaps.geojson', driver='GeoJSON')