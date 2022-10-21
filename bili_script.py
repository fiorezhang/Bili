# -*- coding: utf-8 -*-


# ================================================================  #
#                                                                   #
#                    INTERNAL STUDY ONLY !                          #
#                        VERSION 1.0                                #
#                                                                   #
# ================================================================  #

import os
import sys
import argparse
import json
import tablib
import time
import requests
import urllib.request
import re
import csv
import pandas
import numpy
from io import BytesIO
import gzip
import math

COOKIE = "buvid3=58B237F0-1CF4-D8C2-8B04-B16361B3669252076infoc; b_nut=1666184152; i-wanna-go-back=-1; b_ut=7; _uuid=35CBC1DD-2826-2D1E-CF3F-9484110AB81FE52620infoc; buvid_fp=bccff3f50075981a4479bce7dd4dfe3e; buvid4=5AF04D50-A63E-4DCC-F4B9-D12EA9CC0F4953773-022101920-NByMlaqIUVcBILfZJ+Kk3g%3D%3D; CURRENT_FNVAL=4048; rpdid=|(J|J|kl~~Y|0J'uYYYu)RuY~; PVID=5; is-2022-channel=1; nostalgia_conf=-1; b_lsid=366D3CA7_183F8036564; theme_style=light; sid=6vc4x059; innersign=1"
USERAGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:105.0) Gecko/20100101 Firefox/105.0"
REFERER = "https://www.bilibili.com/"
RETRY = 3
TIMEOUT = 30
MAXREPLY = 10000
MAXVIDEO = 1000

#== MISC ========
def normalizeCountStr(countStr):
    #print(countStr)
    countStr_1 = countStr.replace("-", "0")
    countStr_2 = countStr_1.rstrip('万')
    if countStr_2 == countStr_1:
        count = int(countStr_2)
    else:
        count = int(10000 * float(countStr_2))
    return count
    
#写入CSV文件
def appendCsv(file, row):
    with open(file, mode='a', newline='', encoding='utf-8-sig') as f:
        write=csv.writer(f)
        write.writerow(row)
        f.close

#读出CSV文件
def parseCsv(file):
    rows = []
    with open(file, mode='r', encoding='utf-8-sig') as f:
        read=csv.reader(f)
        rows = [row for row in read]
        f.close
    return rows

#保存文件时候, 去除名字中的非法字符
def validateTitle(title):
    rstr = r"[\t\/\\\:\*\?\"\<\>\|.]"  # '/ \ : * ? " < > |'
#    rstr = r"[\*\?\"\<\>\|]"  # '/ \ : * ? " < > |'
    new_title = re.sub(rstr, r"_", title)  # 替换为下划线
    new_title = new_title.rstrip()
    #new_title = new_title.replace(" ", "")
    return new_title

#创建新目录
def mkdir(path):
    path = path.strip()
    # 判断路径是否存在
    # 存在     True
    # 不存在   False
    isExists=os.path.exists(path)
    #print(isExists)
    # 判断结果
    if not isExists:
        # 如果不存在则创建目录
        # 创建目录操作函数
        os.makedirs(path)
        return True
    else:
        # 如果目录存在则不创建，并提示目录已存在
        return False  

#== Get root replies from video page ========
def getRootReplyFromAidSinglePage(oid, page):
    head = {'User-Agent': USERAGENT, 'cookie': COOKIE, "referer": REFERER}  
    url = "https://api.bilibili.com/x/v2/reply/main?mode=3&next="+str(page)+"&oid="+str(oid)+"&plat=1&type=1"
    #对于每一个视频，爬到动态加载评论的url是这样的，page从1开始计数，上不封顶，但是最后一页以及往后的页面is_end为真，oid即当前页面的aid，可以在原页面通过BVID转换得到。
    
    #print(url)
    retry = RETRY
    while retry > 0:
        try:
            request = urllib.request.Request(url, headers=head)
            response = urllib.request.urlopen(request, timeout=TIMEOUT)
            content = response.read().decode()
            result = json.loads(content)
        except:
            pass
        else:
            break
        retry -= 1
        print("    == Retry getRootReplyFromAidSinglePage(), can fix occational network lag")
        time.sleep(10)
    return result

def getRootReplyFromAidMultiPages(oid):
    #每页评论20条，依此估算最大评论页数
    maxPage = math.ceil(MAXREPLY / 20)
    
    countAll = 0
    repliesAll = []
    for i in range(maxPage): 
        page = i+1 # 页数从1开始，依次递增，直到'is_end'为真时结束
        result = getRootReplyFromAidSinglePage(oid, page)
        #获取总评论数
        if countAll == 0:
            countAll = result["data"]["cursor"]["all_count"]
        #根据'is_end'判断是否最后一页
        is_end = result["data"]["cursor"]["is_end"]
        if is_end == True:
            break            
        #获取当前页的评论
        print("-" * 20 + str(page))
        replies = result["data"]["replies"]
        #print(replies)
        repliesAll.extend(replies)
        time.sleep(1)
    print()
    
    return countAll, repliesAll

#== Get sub replies from root reply(in video page) ========        
def getReplyFromRootSinglePage(oid, rpid, page):
    #https://api.bilibili.com/x/v2/reply/reply?oid=13046277&pn=1&ps=10&root=367309050&type=1
 
    head = {'User-Agent': USERAGENT, 'cookie': COOKIE, "referer": REFERER}  
    url = "https://api.bilibili.com/x/v2/reply/reply?oid="+str(oid)+"&pn="+str(page)+"&ps=10&root="+str(rpid)+"&type=1"
    #对于每一个根评论，爬取下级评论，page从1开始计数，上不封顶，但是最后一页以及往后的页面replies为空，oid即当前根评论对应页面的aid，rpid就是根评论的id
    
    #print(url)
    retry = RETRY
    while retry > 0:
        try:
            request = urllib.request.Request(url, headers=head)
            response = urllib.request.urlopen(request, timeout=TIMEOUT)
            content = response.read().decode()
            result = json.loads(content)
        except:
            pass
        else:
            break
        retry -= 1
        print("    == Retry getReplyFromRootSinglePage(), can fix occational network lag")
        time.sleep(10)
    return result

def getReplyFromRootMultiPages(oid, rpid):
    #每页二级评论10条，依此估算最大评论页数
    maxPage = math.ceil(MAXREPLY / 10)
    
    repliesAll = []
    for i in range(maxPage):
        page = i+1
        result = getReplyFromRootSinglePage(oid, rpid, page)
        replies = result["data"]["replies"]
        #print(replies)
        if replies == None:
            break
        print("    " + "-" * 20 + str(page))
        repliesAll.extend(replies)
        time.sleep(1)
    print()
        
    return repliesAll
 
#== Get AID(OID) the string of number from BVID the string of misc characters ========   
def getAidFromBvid(bvid):
    #原始页面里面能搜到以下字段，其中AID即后面抓取评论要用到的oid，BVID就是URL里面的后缀    __INITIAL_STATE__={"aid":431471681,"bvid":"BV1AG411J7gQ","p"
    head = {'User-Agent': USERAGENT, 'cookie': COOKIE, "referer": REFERER}  
    url = "https://www.bilibili.com/video/"+bvid
    
    #print(url)
    aid = ""
    retry = RETRY
    while retry > 0:
        try:
            request = urllib.request.Request(url, headers=head)
            response = urllib.request.urlopen(request, timeout=TIMEOUT)
            content = response.read()
            buff = BytesIO(content)
            f = gzip.GzipFile(fileobj = buff)
            content = f.read().decode()
            
            pattern = re.compile('__INITIAL_STATE__={"aid":(.*?),"bvid":"(.*?)","p"')
            result = re.search(pattern, content)
            
            aid = result.group(1)
            bvidRead = result.group(2)
            if bvidRead != bvid:
                aid = 0
                print("Missmatch BVID: ", bvid)
        
        except:
            pass
        else:
            break

        retry -= 1
        print("    == Retry getAidFromBvid(), can fix occational network lag")
        time.sleep(10)
        
    return aid

def getTitleFromBvid(bvid):
    head = {'User-Agent': USERAGENT, 'cookie': COOKIE, "referer": REFERER}  
    url = "https://www.bilibili.com/video/"+bvid
    
    #print(url)
    aid = ""
    retry = RETRY
    while retry > 0:
        try:
            request = urllib.request.Request(url, headers=head)
            response = urllib.request.urlopen(request, timeout=TIMEOUT)
            content = response.read()
            buff = BytesIO(content)
            f = gzip.GzipFile(fileobj = buff)
            content = f.read().decode()
            
            pattern = re.compile('<title data-vue-meta="true">(.*?)_哔哩哔哩_bilibili</title>')
            result = re.search(pattern, content)
            
            title = result.group(1)
        
        except:
            pass
        else:
            break

        retry -= 1
        print("    == Retry getTitleFromBvid(), can fix occational network lag")
        time.sleep(10)
        
    return title
    
#== Get videos ordered by view ========
def getVideoFromChannelSinglePage(channel, offset):
    #https://api.bilibili.com/x/web-interface/web/channel/multiple/list?channel_id=6213&sort_type=view&offset=&page_size=30
    head = {'User-Agent': USERAGENT, 'cookie': COOKIE, "referer": REFERER}  
    url = "https://api.bilibili.com/x/web-interface/web/channel/multiple/list?channel_id="+str(channel)+"&sort_type=view&offset="+str(offset)+"&page_size=30"
    
    #print(url)
    retry = RETRY
    while retry > 0:
        try:
            request = urllib.request.Request(url, headers=head)
            response = urllib.request.urlopen(request, timeout=TIMEOUT)
            content = response.read().decode()
            result = json.loads(content)
        except:
            pass
        else:
            break
        retry -= 1
        print("    == Retry getVideoFromChannelSinglePage(), can fix occational network lag")
        time.sleep(10)
    return result    

def getVideoFromChannelMultiPages(channel):
    #每页视频30个，依此估算最大视频页数
    maxVideo = math.ceil(MAXVIDEO / 30)
    
    videosAll = []
    offset = "" #视频第一页offset为空字符串，后面每一页的offset都写在前面一页的信息里
    for i in range(maxVideo):
        result = getVideoFromChannelSinglePage(channel, offset)
        videos = result["data"]["list"]
        #print(videos)
        if result["data"]["has_more"] == True:
            offset = result["data"]["offset"]
        else:
            break
        print("~" * 20 + str(i+1))
        videosAll.extend(videos)
        time.sleep(1)
    print()
        
    return videosAll    

def getTitleFromChannel(channel):
    head = {'User-Agent': USERAGENT, 'cookie': COOKIE, "referer": REFERER}  
    url = "https://www.bilibili.com/v/channel/"+channel
    
    #print(url)
    aid = ""
    retry = RETRY
    while retry > 0:
        try:
            request = urllib.request.Request(url, headers=head)
            response = urllib.request.urlopen(request, timeout=TIMEOUT)
            content = response.read().decode()
            
            pattern = re.compile('<title>(.*?)-哔哩哔哩频道</title>')
            result = re.search(pattern, content)
            
            title = result.group(1)
        
        except:
            pass
        else:
            break

        retry -= 1
        print("    == Retry getTitleFromChannel(), can fix occational network lag")
        time.sleep(10)
        
    return title    
    
#== MAIN ========
def getArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument("--func", type=int, default=0)
    parser.add_argument("--aid", type=str, default="")
    parser.add_argument("--bvid", type=str, default="")
    parser.add_argument("--video", type=str, default="")
    parser.add_argument("--sub", type=int, default=0)
    parser.add_argument("--channel", type=str, default="")
    args = parser.parse_args()
    return args        
        
if __name__ == '__main__':
    args = getArgs()
    FUNC = args.func
    AID = args.aid
    BVID = args.bvid
    VIDEO = args.video
    SUB = args.sub
    CHANNEL = args.channel
    
    if FUNC == 0:#对视频获取评论，生成报表
        #需要得到视频页面的aid，如果给的是网页，剥离尾部的bvid；如果给的是bvid就直接用，根据bvid在页面上提取aid
        title = ""
        if AID == "":
            if VIDEO != "": 
                BVID = VIDEO.split('/')[-1]
            if BVID != "":
                AID = getAidFromBvid(BVID)
                title = getTitleFromBvid(BVID)
        print("Get replies from: " + title + " AID(OID): " + AID)

        #生成表头
        file = "VIDEO_"+str(BVID)+"_"+validateTitle(title)[:20]+"_"+time.strftime("%Y_%m_%d_%H_%M", time.localtime())+".csv"
        rowHead = ["评论ID", "父评论ID", "回复数", "点赞数", "会员ID", "会员名字", "评论"]
        appendCsv(file, rowHead)
        
        #读取评论总数和所有根评论
        countAll, repliesAllRoot = getRootReplyFromAidMultiPages(AID)
        print("Total replies count: ", countAll)
        for replyRoot in repliesAllRoot:
            #逐行写入根评论
            rowReplyRoot = [replyRoot["rpid"], replyRoot["root"], replyRoot["rcount"], replyRoot["like"], replyRoot["member"]["mid"], replyRoot["member"]["uname"], replyRoot["content"]["message"]]
            appendCsv(file, rowReplyRoot)
            
            #对于根评论下还有二级评论的情况
            if SUB == 1 and int(replyRoot["rcount"]) > 0:
                print("Get replies from: " + str(replyRoot["rpid"]))
                #读取二级评论
                repliesAllSub = getReplyFromRootMultiPages(AID, replyRoot["rpid"])
                for replySub in repliesAllSub:
                    #逐行写入二级评论（这样二级评论就跟在了当前根评论后面）
                    rowReplySub = [replySub["rpid"], replySub["root"], replySub["rcount"], replySub["like"], replySub["member"]["mid"], replySub["member"]["uname"], replySub["content"]["message"]]
                    appendCsv(file, rowReplySub)
    elif FUNC == 1: #对频道获取视频，生成报表
        title = getTitleFromChannel(CHANNEL)
        print("Get videos from: " + title)
        #生成表头
        file = "CHANNEL_"+str(CHANNEL)+"_"+validateTitle(title)[:20]+"_"+time.strftime("%Y_%m_%d_%H_%M", time.localtime())+".csv" 
        rowHead = ["视频AID", "视频BVID", "播放数", "点赞数", "时长", "作者ID", "作者名字", "标题"]
        appendCsv(file, rowHead)
        
        videosAll = getVideoFromChannelMultiPages(CHANNEL)
        print("Total videos count: ", len(videosAll))
        for video in videosAll:
            rowVideo = [video["id"], video["bvid"], normalizeCountStr(str(video["view_count"])), normalizeCountStr(str(video["like_count"])), video["duration"], video["author_id"], video["author_name"], video["name"]]
            appendCsv(file, rowVideo)
            