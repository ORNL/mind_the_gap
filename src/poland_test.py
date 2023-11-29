"""Test progression on Poland"""

import auto_tune 

poland = auto_tune.Region('poland',
                          'postgresql://openadmin:openadmin@manhattan:3022/opendb')

#poland.mind(0.02,2,2,20)

#poland.fit_check(1,1)

#poland.gaps.to_file('./poland_test_gaps.geojson', driver='GeoJSON')

poland.prog(build_thresh=0.15, area_floor=0.6,area_ceiling=0.8)

poland.gaps.to_file('poland_test_gaps.geojson', driver = 'GeoJSON')