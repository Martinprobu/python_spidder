#!/usr/bin/env python3
# coding=utf-8
'''
@desc Spider for 国标地址库, python爬虫爬国标地址库
@auth billwu 
@since 20180810
'''
from urllib import request
from bs4 import BeautifulSoup
import sys
import pymysql 
import datetime 
import time 


'''
@desc 去掉字符串右边的所有0 
'''
def removeRezerZero(strSour):
    j = len(strSour)
    for i in range(len(strSour)-1,-1,-1):
        if strSour[i] != '0':
            break;
        j = i
    #12011特殊情况
    #if strSour[0:j] == '12011':
    #    return strSour[0:6];
    #elif strSour[0:j] == '13011':
    #    return strSour[0:6];
    if j == 5:
        return strSour[0:6];
    #位数小于4特殊情况
    if j <= 3:
        return strSour[0:4] 
    return strSour[0:j]
#print(removeRezerZero('120110000000'))
#sys.exit()

'''
@desc 查询db里是否有此数据，有则返回true,否则返回false
'''
def exisfAreaCode(cursor, pro_code):
    sele_sql = 'select area_code from m_area_dev where area_code = "%s"'
    list = cursor.execute(sele_sql % (pro_code))
    #list = cursor.fetchall()
    #print(list)
    if list:
        #print("true")
        return True
    else:
        #print("false")
        return False

##=============================================================================业务代码 Start ======================
url = "http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2017/"
print("Strt Addr Spider ...")

insert_flag = True
sleep_time = 5
sleep_time_sub = 0.2
db = pymysql.connect("127.0.0.1","root","123456","ins_prometheus_saas")
cursor = db.cursor()
#area_code, area_name, area_type, parent_code, create_time, full_name, outer_area_code
insert_sql = 'insert into m_area_dev values("%s", "%s", "%d", "%s", "%s", "%s", "%s")'
create_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'}
page = request.Request(url,headers=headers)
page_info = request.urlopen(page).read().decode('gbk')
soup = BeautifulSoup(page_info, 'html.parser')
#titles = soup.find_all('a', 'href')

#循环一级地址,如广东省 ======================================
provi1 = soup.findAll("tr",{'class':'provincetr'}, limit=4)
for prov in provi1:
    time.sleep(sleep_time)
    a1 = prov.findAll('a')
    for a in a1:
        href_val = a.get('href')
        #循环二级地址i,如广州市 ========================================
        pro_name = a.text.strip()               ##省份名称
        pro_code = href_val.split('.')[0]       ##省份编码
        
        #自定义筛选黑名单(即已经挖过的数据略过) =====================================
        #black_list = ['11', '12', '13']
        black_simple_code = 61
        black_simple_code_l4 = 620826000000 #第4级的筛选
        #if pro_code in black_list:
        if int(pro_code) <= black_simple_code:
            print('black_list == pro_code ================= ' + pro_code)
            continue

        url2 = url + pro_code + ".html"         ##市列表url
        if insert_flag and not exisfAreaCode(cursor, pro_code):
            cursor.execute(insert_sql % (pro_code, pro_name, 1, 0, create_time, pro_name, ''))  #============== insert db
        #area_code, area_name, area_type, parent_code, create_time, full_name, outer_area_code
        
        #print(url2)        
        page2 = request.Request(url2,headers=headers)
        page_info2 = request.urlopen(page2).read().decode('gbk')
        soup2 = BeautifulSoup(page_info2, 'html.parser')
        city2 = soup2.findAll("tr",{'class':'citytr'})
        for city in city2:
            time.sleep(sleep_time_sub)
            #city_code = city.a.text           ##语法可以这样写，代码获取第一个， ##市编码
            city_sele  = city.select('a')
            city_code = city_sele[0].text       ##市编码
            city_name = city_sele[1].text       ##市名称
            if insert_flag and not exisfAreaCode(cursor, city_code):
                cursor.execute(insert_sql % (city_code, city_name, 2, pro_code, create_time, pro_name + city_name, ''))  #========== insert db
                db.commit()     ##每一个三级地址都commit一次 ====
            #print(city_code)
            #循环三级地址,如天河区 ================================================
            url3 = url + pro_code + "/" + removeRezerZero(city_code) + ".html"         ##列表url
            print(url3)        
            page3 = request.Request(url3,headers=headers)
            page_info3 = request.urlopen(page3).read().decode('gbk')
            soup3 = BeautifulSoup(page_info3, 'html.parser')
            city3  = soup3.findAll("tr",{'class':'countytr'})
            for town in city3:
                time.sleep(sleep_time_sub)
                town_sele  = town.select('a')
                if not town_sele or '市辖区' == town_sele[1].text:
                    continue
                town_code = town_sele[0].text       ##市/镇编码
                town_name = town_sele[1].text       ##市/镇名称
                if insert_flag and not exisfAreaCode(cursor, town_code):
                    cursor.execute(insert_sql % (town_code, town_name, 3, city_code, create_time, pro_name + city_name + town_name, ''))           #============================= insert_sql
                print(town_code + town_name)
                ##循环四级地址,如创业街 ============================================================
                if int(town_code) <= black_simple_code_l4:
                    print('black_list l4 == town_code ================= ' + town_code)
                    continue
                if len(city3) <= 1 and town_code != '419001000000':     ##特殊条件  
                    url4 = url + pro_code + "/" + removeRezerZero(town_code) + ".html"         ##列表url
                else:
                    url4 = url + pro_code + "/" + town_code[2:4] + "/" + removeRezerZero(town_code) + ".html"         ##列表url
                print(url4)        
                page4 = request.Request(url4,headers=headers)
                page_info4 = request.urlopen(page4).read().decode('gbk')
                soup4 = BeautifulSoup(page_info4, 'html.parser')
                city4  = soup4.findAll("tr",{'class':'towntr'})
                for stri in city4:
                    time.sleep(sleep_time_sub)
                    stri_sele  = stri.select('a')
                    if not stri_sele:
                        continue
                    stri_code = stri_sele[0].text       ##镇/街道编码
                    stri_name = stri_sele[1].text       ##镇/街道名称
                    if insert_flag and not exisfAreaCode(cursor, stri_code):
                        full_name4 = pro_name + city_name + town_name + stri_name
                        cursor.execute(insert_sql % (stri_code, stri_name, 4, town_code, create_time, full_name4, ''))       #============================= insert_sql
                        print(full_name4)

#提交事务, 这里默认不做rollback, 不捕捉异常
db.commit()
db.close()

print("End Addr Spider ... ")




