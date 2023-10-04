"""Test progression on Poland"""

import progress

poland = progress.country('poland',
                          'postgresql://openadmin:openadmin@manhattan:3022/opendb')

#poland.mind(0.02,2,2,20)

#poland.fit_check(1,1)

#poland.gaps.to_file('./poland_test_gaps.geojson', driver='GeoJSON')

poland.prog()