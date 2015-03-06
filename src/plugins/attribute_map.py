# -*- coding: utf-8 -*-
import numpy as np
import sys
import pandas as pd
import re
import os
from country_data import country_map

path_file = os.path.expandvars("$CHINA_FOLDER/2001-01.txt")
path_attr = os.path.expandvars("$CHINA_FOLDER/chinese_attrs.txt")

df = pd.read_csv(path_file, encoding="utf-16")
attrs = pd.read_csv(path_attr, encoding="utf-16", sep="\t")

_ascii_letters = re.compile(r'[a-zA-Z),0-9]', flags=re.UNICODE)

def only_nonascii(text):
    return _ascii_letters.sub("", text)

provinces = attrs.values

lookup = {}
lookup[u"未知"] = "xxxxxx"
lookup[u"宁夏回族石嘴山"] = "640200"
lookup[u"宁夏回族银南"] = "640103"
lookup[u"宁夏回族银川"] = "640100"
lookup[u"宁夏回族其它"] = "640xxx"
lookup[u"西藏自治区日喀则"] = "542300"
lookup[u"宁夏回族固原"]= "642200"
# special prov
lookup[u"新疆维吾尔哈密"] = "652200"
lookup[u'新疆维吾尔和田'] = "653200"
lookup[u'新疆维吾尔其它'] = "65xxxx"
lookup[u'新疆维吾尔石河子'] = "659001"
lookup[u'新疆维吾尔喀什'] = "653100" #very violent
lookup[u'新疆维吾尔巴音'] = "652800"
lookup[u'新疆维吾尔乌鲁木齐'] = "650100"
lookup[u'新疆维吾尔吉昌'] = "652301"
lookup[u'新疆维吾尔吐鲁番'] = "652100"
lookup[u'新疆维吾尔塔城'] = "654200"
lookup[u'新疆维吾尔阿勒泰'] = "654300"
lookup[u'新疆维吾尔伊宁'] = "654101"
lookup[u'新疆维吾尔博乐'] = "652701"
lookup[u'新疆维吾尔克孜'] = "653000"
lookup[u'新疆维吾尔克拉玛依'] = "650200"
lookup[u'新疆维吾尔阿克苏'] = "652900"
# special prov
lookup[u'内蒙古自治区其它'] = "15xxxx"
lookup[u'内蒙古自治区呼和浩特'] = "150100"
lookup[u'内蒙古自治区巴彦淖尔盟'] = "152800"
lookup[u'内蒙古自治区哲里木盟'] = "152300"
lookup[u'内蒙古自治区兴安盟'] = "152200"
lookup[u'内蒙古自治区乌海'] = "150300"
lookup[u'内蒙古自治区伊克昭盟'] = "152700"
lookup[u'内蒙古自治区呼伦贝尔盟'] = "152100"
lookup[u'内蒙古自治区满州里'] = "152102"
lookup[u'内蒙古自治区包头'] = "150200"
lookup[u'内蒙古自治区二连'] = "152501"
lookup[u'内蒙古自治区阿拉善盟'] = "152900"
lookup[u'内蒙古自治区乌兰察布盟'] = "152600"
lookup[u'内蒙古自治区赤峰'] = "150400"
# special non-province cities
lookup[u'厦门特区'] = "350200"
lookup[u'深圳特区'] = "440300"
lookup[u'上海市南市'] = '310100'
lookup[u'上海市其它'] = '31xxxx'
lookup[u'上海市浦东'] = '310115'
lookup[u'北京市其它'] = '11xxxx'
lookup[u'北京市经济技术开发区'] = '110115'
lookup[u'北京市通县']= '110112'

lookup[u'天津经济技术开发区'] = '120107'

lookup[u'山东省渮泽'] = '372900' 
lookup[u'天津市西郊'] = '120100'
lookup[u'天津市东郊'] = '120100'
lookup[u'天津市南郊'] = '120100'
lookup[u'天津市北郊'] = '120100'
lookup[u'天津市南开'] = '120104'
lookup[u'天津港保税区'] = '120109'
lookup[u'天津市其它'] = '12xxxx'
lookup[u'河北省石家庄'] = '130100'
lookup[u'河北省其它'] = '13xxxx'
lookup[u'黑龙江省大庆'] = '230600'
lookup[u'上海浦东新区']= '310115'

lookup[u'长春经济技术开发区'] = '220100'
lookup[u'吉林省梅河口'] = '220600'
lookup[u'贵州省六盘山'] = '520200'

lookup[u'吉林高新技术产业开发区'] = '220200'
lookup[u'江苏昆山出口加工区'] = '320583'
lookup[u'宁波北仓港保税区'] = '330200'
lookup[u'深圳福田保税区'] ='440300'
lookup[u'黑龙江省鸡西'] ='230300'
lookup[u'珠海经济特区'] ='440400'
lookup[u'贵州省黔南'] ='522700'
lookup[u'黑龙江省同江'] ='230881'
lookup[u'海南经济特区'] ='460100'
lookup[u'杭州高新技术产业开发区'] ='330100'
lookup[u'济南高技术产业开发区'] ='370100'

lookup[u'杭州高新技术产业开发区'] ='330100'
lookup[u'杭州高新技术产业开发区'] ='330100'
lookup[u'杭州高新技术产业开发区'] ='330100'
lookup[u'沈阳经济技术开发区'] = '210100'
lookup[u'沈阳南湖科技开发区'] = '210100'
lookup[u'大连经济技术开发区'] = '210200'
lookup[u'上海漕河泾新兴技术开发区']='310112'
lookup[u'宁波经济技术开发区'] = '330200'
lookup[u'上海经济技术开发区'] = '310100'
lookup[u'上海闵行经济技术开发区'] = '310112'
lookup[u'南京浦口高新技术外向型开发区'] = '320100'
lookup[u'苏州高新技术产业开发区'] = '320500'
lookup[u'大连大窟湾保税区'] = '210200'
lookup[u'上海外高桥保税区'] = '310115'
lookup[u'厦门象屿保税区'] ='350200'
lookup[u'江苏张家港保税区'] = '320582'
lookup[u'山西省雁北'] ='140600'
lookup[u'安徽省其它']= '34xxxx'
lookup[u'青海省海东']='632100'
lookup[u'海南其它经济特区']='46xxxx'
lookup[u'天津市蓟县'] = '120225'
lookup[u'天津市蓟县'] = '120225'
lookup[u'江苏省其它'] = '32xxxx'
lookup[u'吉林省公主岭'] = '220381'
lookup[u'吉林省浑江'] = '220600'
lookup[u'汕头特区汕头'] = '440500'
lookup[u'河南省济源'] = ''
lookup[u'珠海特区'] = '440400'
lookup[u'重庆市江津市'] = '500381'
lookup[u'重庆市合川市'] = '500382'
lookup[u'重庆市南川市'] = '500384'
lookup[u'上海市川沙']='310115'
lookup[u'北京市朝阳']='110105'
lookup[u'北京市朝阳'] = '110105'
lookup[u'内蒙古自治区锡林郭勒盟']= '152500'
lookup[u'重庆市壁山'] ='500383'
lookup[u'重庆市永川市'] = '500383'
lookup[u'重庆市酉阳土家族苗族自治'] = '500242'
lookup[u'四川省其它'] = '51xxxx'
lookup[u'黑龙江省其它'] = '23xxxx'
lookup[u'BEIJING'] = '110000'
lookup[u'SHANDONGYANTAI'] = '370600'
lookup[u'LIAOLINGYINKOU'] = '210800'

# TODO FIX:
lookup[u'重庆市石柱土家族苗族自治'] = '500240'
lookup[u"重庆市彭水苗族土家族自治"] = "500243"
lookup[u"安徽毫州"] = "341281"
lookup[u"广西地区"] = "45xxxx"
lookup[u"广西港市"] = "450602"
lookup[u'重庆市秀山土家族苗族自治'] = '500241'
lookup[u'重庆市黔江土家族苗族自治'] = '500239'

mylist = []

special_zone = {}


def clean(city):
    city = city.replace(u'市', '')
    city = city.replace(u'县', '')
    city = city.replace(u'区', '')
    city= city.replace(u'自治', '')
    return city

for x in provinces:
    orig_province = x[1] 
    orig_city = x[3]
         
    province = x[1][:3].strip()
    city = x[3].strip()
    
    city  = only_nonascii(city)
    
#    if province.startswith(u"新疆"):
#        print province

    city = city.replace(u'市', '')
    city = city.replace(u'县', '')
    city = city.replace(u'区', '')
    city= city.replace(u'自治', '')
    
    if province[:2] in [u"西藏", u"广西", u"宁夏"]:
        if u'石嘴山区自治区' == orig_city.strip():
            city = city[:3]
        else:
            city = city[:2]
        province = province[:2]
    elif province[:3] in [u'重庆市', u'上海市', u'北京市']:
        province = province[:3]
    elif province[:3] in [u'吉林省', u'山西省', u'云南省']:
        province = province[:3]
        city = city[:2]

    if len(str(x[5])) != 6 or "XX" in str(x[5]):
        print  x
        raise Exception("Invalid geo code")
    pid = (province + city).strip()
    lookup[pid] = x[5]
    # if u"北京" in city:
        # print city[:2], "here", 
        # sys.exit()
    # print "SPECIAL DICTIONARY"
    # print city[:2], x[-1]
    special_zone[city[:2].strip()] = x[5]
    #print u"{} = {}".format(pid, x[-1])

#print new_dict
#sys.exit()
tmp_list = []
data = df.values
special = set([])


def find_zip(location):
    # if not location:
    if len(unicode(location))  == 0:
        return "xxxxxx"
    # raise Exception("WHJO?", location)
    if type(location) != float:
        location = location.strip()
    
    if type(location) == float:
        # -- No Location Information
        return "xxxxxx"
    elif location in lookup:
        # print lookup[location]
        return lookup[location]
    elif location[:2] in [u"西藏", u"广西"]:
        province = location[:2]
        if location == u"宁夏回族其它":
            city = location[-3:]
        else:
            city = location[-2:]
        if city == u'其它':
            base_map = {u"西藏":"54", u"广西":"45"}
            return base_map[province] + u"xxxx"
        pid = province + city
        return lookup[pid]
    elif location[:3] in [u'重庆市', u'上海市', u'北京市']:
        # print location
        if location.endswith(u"区") or location.endswith(u"县"): # or  location.endswith(u"市"):
            location = location[:-1]
        # print location
        return lookup[location]
    # elif location[:3] in [u'河北省']:
        # return lookup[location]
    elif location.endswith(u'开发区') and not location.startswith(u"北京新"):
         for k in special_zone.keys():
            if location[:2].strip() in k:
                return special_zone[k]
    elif location.endswith(u'保税区'):
        for k in special_zone.keys():
            if location[:2].strip() in k:
                return special_zone[location[-5:-3].strip()]
    elif location.startswith(u'四川省'):
        for k in special_zone.keys():
            if location[3:5].strip() in k:
                return special_zone[location[3:5].strip()]
    elif location.startswith(u'黑龙江省'):
        for k in special_zone.keys():
            if location[4:6].strip() in k:
                # print location
                # print location[4:6]
                # print special_zone[]
                return special_zone[location[4:6].strip()]
    if len(location) >= 5:
        kays = {}
        val = "xxxxxx"
        for k in special_zone.keys():
            if location[3:5].strip() in k:
                # print location
                # print location[-5:-3]
                val = special_zone[location[3:5].strip()]
                kays[k] = [location, val]
        if len(kays.keys()) > 1:
            raise Exception("More than one")
        return val
    elif location[2:] in special_zone:
        return special_zone[location[:2]]

    # print "ELSE: ", location
    tmp_list.append(location)
    raise Exception("BAD location:", location)

def find_wld(name_cn):
    # print name_cn
    try:
        return country_map[name_cn]
    except:
        print "BIG FAIL!!!!!"
        print name_cn
        print
        return "xxxxx"

# for idx,x in enumerate(data):
    # quantity,i,hs,amount,location,exportdest,inbet,imp = x
    # find_zip(location)

# for x in set(tmp_list):
#     print "missing rule for", x
# print len(set(tmp_list)), "missing locations"

