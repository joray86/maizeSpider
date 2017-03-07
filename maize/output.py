# coding=utf-8
__author__ = 'JoRay'

import csv
import json
import re
import pandas as pd
import datetime
import seaborn as sns
import matplotlib.pyplot as plt
import pymysql
import pymysql.cursors

with open("C:\\tools\\priceData.json", 'r', encoding='utf-8') as f:
    # text = f.read()
    priceAll = {}
    for text in f.readlines():
        price_dict = json.loads(text)  # movie_dict是list

        title = price_dict["title"][0].replace(u'\xa0', u'')
        pattern = re.compile(r'\d+')
        datelist = re.findall(pattern, title)
        # date = '' + datelist[0] + '-' + datelist[1] + '-' + datelist[2]
        date = datetime.datetime(int(datelist[0]),int(datelist[1]),int(datelist[2]))
        link = price_dict["link"]
        priceDesc = price_dict["priceDesc"]

        # companyList = ['寿光金玉米淀粉', '寿光新丰淀粉', '昌乐盛泰药业', '滨州市邹平西王', '滨州市邹平华义', '滨州金汇', '滨州博兴香驰', \
        #                '德州市平原福洋生物', '德州禹城保龄宝', '临沂市沂水大地玉米', '临沂市鲁洲集团山东公司山东公司', '临沂市沂水青援', \
        #                '临沂市沂南地区饲料厂', '山东泰安东平祥瑞药业']

        companyList1 = ['潍坊市寿光金玉米淀粉','潍坊寿光新丰淀粉','潍坊市诸城兴贸淀粉','潍坊市昌乐盛泰','潍坊市昌乐英轩实业',\
                        '聊城市临清金玉米','滨州市邹平西王','滨州金汇','德州乐陵市中谷淀粉','德州市平原福洋生物',\
                        '德州禹城保龄宝','临沂市沂水大地玉米','临沂市鲁洲集团山东公司','临沂市沂水青援淀粉',\
                        '山东泰安东平祥瑞药业','恒仁工贸有限公司','菏泽成武大地']
        companyList2 = ['德州地区粮点新玉米收购价格','潍坊地区饲料厂','临沂地区饲料厂','寿光地区深加工']
        companyList3 = ['青岛地区贸易商','饲料企业']
        companyList4 = ['聊城地区贸易商','饲料企业','深加工企业']
        companyList5 = ['临沂市沂水地区饲料厂']
        # 做数据的分析
        priceDict = {}
        for line in priceDesc:
            # line.replace(u'\xa0', u'')

            # 第一类企业，以XX元/斤为单位
            for company in companyList1:
                pattern = re.compile(r'' + company + '.*?(\d+.\d*)元/斤')
                result = re.search(pattern, line)
                if(result):
                    # print(company + ': ' + result.group(1))
                    priceDict[company] = float(result.group(1))

            # 第一类企业的特殊情况，以XX-XX元/斤为单位，取二者的平均值
            for company in companyList5:
                pattern = re.compile(r'' + company + '.*?(\d+.\d*)元/斤')
                pattern_special = re.compile(r'' + company + '.{0,8}?(\d+.\d*)-(\d+.\d*)元/斤')
                result_speical = re.search(pattern_special, line)
                if (result_speical):
                    # print(company + ': ' + result.group(1))
                    priceDict[company] = (float(result_speical.group(1))+float(result_speical.group(2)))/2
                    # print(company)
                    # print(line)
                    # print(result_speical.group(1)+","+result_speical.group(2))
                    # print((float(result_speical.group(1))+float(result_speical.group(2)))/2)
                else:
                    result = re.search(pattern, line)
                    if(result):
                        # print(company + ': ' + result.group(1))
                        priceDict[company] = float(result.group(1))

                    # 第二类企业，以XX元/吨为单位
            # 有XXXX-XXXX元/吨的情况，做特殊处理(如果不是4位数，是错误的数据)
            for company in companyList2:
                pattern = re.compile(r'' + company + '.*?(\d*)元/吨')
                pattern_special = re.compile(r'' + company + '.{0,8}?(\d{4})-(\d{4})元/吨')
                result_speical = re.search(pattern_special, line)
                if (result_speical):
                    priceDict[company] = (float(result_speical.group(1))+float(result_speical.group(2))) / 4000
                else:
                    result=re.search(pattern, line)
                    if(result):
                        # print(company + ': ' + result.group(1))
                        priceDict[company] = float(result.group(1))/2000

            # 第三类企业，以XX元/吨为单位，先贸易商后饲料企业
            pattern = re.compile(r'' + companyList3[0] + '.*?(\d*)元/吨' + '.*?' + companyList3[1] + '.*?(\d*)元/吨')
            pattern_special1 = re.compile(r'' + companyList3[0] + '.*?(\d{4})-(\d{4})元/吨' + '.*?' + companyList3[1] + '.*?(\d*)元/吨')
            pattern_special2 = re.compile(r'' + companyList3[0] + '.*?(\d*)元/吨' + '.*?' + companyList3[1] + '.*?(\d{4})-(\d{4})元/吨')
            pattern_special3 = re.compile(r'' + companyList3[0] + '.*?(\d{4})-(\d{4})元/吨' + '.*?' + companyList3[1] + '.*?(\d*)元/吨')

            result = re.search(pattern, line)
            result_special1 = re.search(pattern_special1, line)
            result_special2 = re.search(pattern_special2, line)
            result_special3 = re.search(pattern_special3, line)

            if (result):
                # print(company + ': ' + result.group(1))
                priceDict[companyList3[0]] = float(result.group(1)) / 2000
                priceDict['青岛地区' + companyList3[1]] = float(result.group(2)) / 2000
            elif (result_special1):
                priceDict[companyList3[0]] = (float(result_special1.group(1))+float(result_special1.group(2))) / 4000
                priceDict['青岛地区' + companyList3[1]] = float(result_special1.group(3)) / 2000
            elif(result_special2):
                priceDict[companyList3[0]] = float(result_special2.group(1)) / 2000
                priceDict['青岛地区' + companyList3[1]] = (float(result_special2.group(2))+float(result_special2.group(3))) / 4000
            elif(result_special3):
                priceDict[companyList3[0]] = (float(result_special3.group(1))+float(result_special3.group(2))) / 4000
                priceDict['青岛地区' + companyList3[1]] = (float(result_special3.group(3)) + float(result_special3.group(4))) / 4000

            # 第四类企业，以XX元/吨为单位，先贸易商后饲料企业，后深加工企业
            pattern = re.compile(r'' + companyList4[0] + '.*?(\d*)元/吨' + '.*?' + companyList4[1] + '.*?(\d*)元/吨'\
                                 +'.*?' + companyList4[2] + '.*?(\d*)元/吨')
            result = re.search(pattern, line)
            if (result):
                # print(company + ': ' + result.group(1))
                priceDict[companyList4[0]] = float(result.group(1))/2000
                priceDict['聊城地区'+ companyList4[1]] = float(result.group(2))/2000
                priceDict['聊城地区' + companyList4[2]] = float(result.group(3))/2000

        # print(line)

        # str和Unicode不能混用，要么将Unicode类型encode为其他编码，要么将str类型decode为其他编码
        # python的内部使用Unicode，str如“电影： ”是字节串，由Unicode经过编码(encode)后的字节组成的
        # 与下句不同的另一种组合字符串方式：print "电影: " + title.encode("utf-8")
        # print(date)
        # print(link)
        # print(priceDesc)
        priceAll[date] = priceDict

    dfPrice = pd.DataFrame(priceAll)
    priceIndex = [
        '潍坊市寿光金玉米淀粉',
        '潍坊寿光新丰淀粉',
        '潍坊市诸城兴贸淀粉',
        '潍坊市昌乐盛泰',
        '潍坊市昌乐英轩实业',
        '临沂市沂水地区饲料厂',
        '聊城市临清金玉米',
        '滨州市邹平西王',
        '滨州金汇',
        # '德州乐陵市中谷淀粉',
        '德州市平原福洋生物',
        '德州禹城保龄宝',
        '德州地区粮点新玉米收购价格',
        '临沂市沂水大地玉米',
        '临沂市鲁洲集团山东公司',
        '临沂市沂水青援淀粉',
        '临沂地区饲料厂',
        '山东泰安东平祥瑞药业',
        '恒仁工贸有限公司',
        '菏泽成武大地',
        '青岛地区贸易商',
        '青岛地区饲料企业',
        '聊城地区贸易商',
        '聊城地区饲料企业',
        '聊城地区深加工企业',
        '潍坊地区饲料厂',
        '寿光地区深加工'
    ]
    # print(dfPrice)
    dfPriceOrder = pd.DataFrame(dfPrice,priceIndex)
    dfPriceOrder.fillna(0)

    dfPriceOrder.to_excel(r'0234.xls',sheet_name='Sheet1')

    # sns.distplot(dfPriceOrder[0:1])
    # sns.plt.show()

# with open('eggs.csv', 'w', newline='', encoding='utf-8', errors='ignore') as csvfile:
#     spamwriter = csv.writer(csvfile, delimiter='\n')
#     titlelist = []
#     titlelist.append(title)
#     spamwriter.writerow(titlelist)
#     # spamwriter.writerow(link)
#     spamwriter.writerow(priceDesc)

# with open('eggs.csv', 'w', newline='') as csvfile:
#     spamwriter = csv.writer(csvfile, delimiter=' ',
#                             quotechar='|', quoting=csv.QUOTE_MINIMAL)
#     spamwriter.writerow(['Spam'] * 5 + ['Baked Beans'])
#     spamwriter.writerow(['Spam', 'Lovely Spam', 'Wonderful Spam'])

    #连接数据库，df是二维表，不知道怎么存入数据库。
    # dbargs = {
    #     'host':'localhost',
    #     'port':3306,
    #     'db':'local_test',
    #     'user':'root',
    #     'password':'glowfly17',
    # }
    # conn = pymysql.connect(**dbargs)