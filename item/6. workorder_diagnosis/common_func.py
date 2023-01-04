import math
'''
公共函数调用
'''
##基于坐标点经纬度计算距离
def calc_greate_circle_distance(lola1,lola2):
    lat1 = lola1[1]
    lon1 = lola1[0]
    lat2 = lola2[1]
    lon2 = lola2[0]
    R=6371000
    lat1_rad = lat1 * math.pi / 180
    lon1_rad = lon1 * math.pi / 180
    lat2_rad = lat2 * math.pi / 180
    lon2_rad = lon2 * math.pi / 180
    d = math.sqrt(math.pow(math.cos((lat1_rad + lat2_rad) / 2) * R * (lon1_rad - lon2_rad), 2) +
                  math.pow(R * (lat1_rad - lat2_rad), 2))
    if d < 10000:
        return float(d)
    else:
        delta_lat = lat2_rad - lat1_rad
        delta_lon = lon2_rad - lon1_rad
        a = math.sin(delta_lat / 2) * math.sin(delta_lat / 2) + \
            math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) * math.sin(delta_lon / 2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        return float(R * c)
