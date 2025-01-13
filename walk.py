#!/usr/bin/python
# -*- coding: utf-8 -*-

from ast import If
import os
import csv
import datetime
import pandas as pd
import requests
import locale
import shutil
from ftplib import FTP_TLS
from datetime import date,timedelta

# 25/01/13 v1.34 年間の週移動平均ランキング追加 
version = "1.34"       
debug = 0     #  1 ... debug
appdir = os.path.dirname(os.path.abspath(__file__))

dailyfile = appdir + "./daily.txt"
templatefile = appdir + "./template.htm"
resultfile = appdir + "./walk.htm"
conffile = appdir + "\\walk.conf"
logfile = appdir + "\\walk.log"
data_bak_file = appdir + "./walkdata.txt"

#  統計情報  {キー  yymm  : 値   辞書   キー max min ave  maxdate mindate}
statinfo = {}
allinfo = {}

datelist = []
steplist = []
yymm_list = []
ave_list = []
max_list = []
min_list = []
ftp_host = ftp_user = ftp_pass = ftp_url =  ""
df = ""
out = ""
logf = ""
pixela_url = ""
pixela_token = ""

lastdate = ""    #  最終データ日付   datetime.date型
datafile = ""
allrank = ""     #  歩数ランキング
monrank = ""     #  歩数ランキング  今月
dailyindex = []  #  毎日のグラフ日付
dailystep  = []  #  毎日のグラフ歩数
lasthh = 0       #  何時までのデータか
yearinfo = {}    #  年ごとの平均

def main_proc():
    global  datafile,logf,end_year
    locale.setlocale(locale.LC_TIME, '')
    logf = open(logfile,'a',encoding='utf-8')
    logf.write("\n=== start %s === \n" % datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
    
    read_config()
    date_settings()
    end_year = today_yy    #  データが存在する最終年
    if datafile == "" :
        datafile = data_bak_file
    if not os.path.isfile(datafile) :
        logf.write("data file not found \n")
        logf.write("\n=== end   %s === \n" % datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
        logf.close()
        return
    read_data()
    create_dataframe()
    calc_move_ave()
    post_pixela()
    parse_template()
    ftp_upload()
    if debug == 0 :
        shutil.copyfile(datafile, data_bak_file)
        os.remove(datafile)
    logf.write("\n=== end   %s === \n" % datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
    logf.close()


def read_data():
    global datelist,steplist,lasthh

    out = open(dailyfile,'w',  encoding='utf-8')
    line = 0 
    curhh = 0

    with open(datafile) as myfile:     # 行数をカウント  最終データの判断に使用
        total_lines = sum(1 for line in myfile)
    with open(datafile) as f:
        reader = csv.reader(f)
        for row in reader:
            line = line + 1 
            if line <= 2 :     #  先頭2行はヘッダなのでスキップ
                continue 
            cnt = 0 
            prev_lasthh = curhh    # 前日の最終データ 時
            curhh = 0  
            for i in range(1,25):     # 24時間分のデータを合計する
                cnt = cnt + int(row[i] )
                if int(row[i] ) != 0 :
                    curhh = i -1      #  本データの時間をセット
            #  最終データが 12時までしかない場合は集計に含めない
            if line == total_lines and curhh <= 12 :
                lasthh = prev_lasthh       # 前日の最終時間
                break
            lasthh = curhh        # 最終時間のセット
            dt = datetime.datetime.strptime(row[0], '%Y%m%d')
            datelist.append(dt)
            steplist.append(cnt)
            dd = dt.strftime('%Y/%m/%d')
            out.write(f"{dd},{cnt}\n")    #  日付,歩数   の形式

    out.close()

def create_dataframe() :
    global df,yymm_list,ave_list,max_list,min_list,lastdate,allinfo,allrank,monrank,yearrank
    global dailyindex,dailystep,quar_name,quar_ave
    df = pd.DataFrame(list(zip(datelist,steplist)), columns = ['date','step'])
    df.set_index('date', inplace=True) 

    #  月別集計
    #  m_ave は { 'step' : {データ} } で データは {'日付': 歩数 } の形式
    m_ave = df.resample(rule = "M").mean().to_dict()
    m_max = df.resample(rule = "M").max().to_dict()
    m_min = df.resample(rule = "M").min().to_dict()

    q = df.resample(rule = "Q").mean().to_dict()
    s = q['step']
    quar_name = s.keys()
    quar_ave = s.values()

    #  データ部分を取り出す
    s  = m_ave['step']
    yymm_list = s.keys()
    ave_list = s.values()
    s  = m_max['step']
    max_list = s.values()
    s  = m_min['step']
    min_list = s.values()

    lastdate = df.tail(1).index.date[0]

    monthinfo = {}
    dfyymm = ""
    data_exist = 0 
    for yy in range(2021, end_year+1) :    # 2021年 ～ 2024年
        dfyy = df[df.index.year == yy]
        yearinfo[yy] = dfyy.mean()['step']
        for mm  in range(1,13) :     #  1月 ～ 12月
            yymm = yy*100+mm
            cur_dfyymm = dfyymm
            dfyymm = dfyy[dfyy.index.month == mm]
            if len(dfyymm) == 0 :
                if data_exist == 0 :   # まだデータがない
                    continue
                else :
                    break              # データ終わり
            
            data_exist = 1 
            monthinfo = {}
            monthinfo['mean'] = dfyymm.mean()['step']
            monthinfo['median'] = dfyymm.median()['step']
            monthinfo['std'] = dfyymm.std()['step']
            monthinfo['max'] = dfyymm.max()['step']
            monthinfo['maxdate'] = dfyymm.idxmax()['step'].strftime('%m/%d %a')
            monthinfo['min'] = dfyymm.min()['step']
            monthinfo['mindate'] = dfyymm.idxmin()['step'].strftime('%m/%d %a')
            statinfo[yymm] = monthinfo

    # for を抜けた cur_dfyymm が今月のデータ
    sortstep = cur_dfyymm.sort_values('step',ascending=False)
    monrank = sortstep.head(10)   #  今月のランク

    allinfo['mean'] = df.mean()['step']
    allinfo['median'] = df.median()['step']
    allinfo['std'] = df.std()['step']
    allinfo['max'] = df.max()['step']
    allinfo['min'] = df.min()['step']
    allinfo['maxdate'] = df.idxmax()['step'].strftime('%m/%d %a')
    allinfo['mindate'] = df.idxmin()['step'].strftime('%m/%d %a')

    sortstep = df.sort_values('step',ascending=False)
    allrank = sortstep.head(20)

    df_tail31 = df.tail(31)
    s = df_tail31['step']
    dailyindex = s.keys()
    dailystep = s.values.tolist()

    last365 = df.tail(365)
    sortstep = last365.sort_values('step',ascending=False)
    yearrank = sortstep.head(20)   #  365日間のランク
    last30 = df.tail(30)
    monrank = last30.sort_values('step',ascending=False)
    monrank = monrank.head(10)

#   月ごとの平均値のトップを表示
def month_ave_top() :
    df_mon = df.resample(rule = "M").mean()
    df_mon = df_mon.sort_values('step',ascending=False)
    month_top_com(df_mon)

#   月ごとの中央値のトップを表示
def month_median_top() :
    df_mon = df.resample(rule = "M").median()
    df_mon = df_mon.sort_values('step',ascending=False)
    month_top_com(df_mon)

#   月ごとの最大値のトップを表示
def month_max_top() :
    df_mon = df.resample(rule = "M").max()
    df_mon = df_mon.sort_values('step',ascending=False)
    month_top_com(df_mon)

#   月ごとの最小値のトップを表示
def month_min_top() :
    df_mon = df.resample(rule = "M").min()
    df_mon = df_mon.sort_values('step',ascending=False)
    month_top_com(df_mon)

def month_top_com(df_top) :
    i = 0 
    for index,row in df_top.head(5).iterrows() :
        i += 1
        date_str = index.strftime('%Y/%m')
        if index.year == lastdate.year and  index.month == lastdate.month :
            date_str = f'<span class=red>{date_str}</span>'
        out.write(f'<tr><td align="right">{i}</td><td>{row["step"]:5.0f}</td><td>{date_str}</td></tr>')



def calc_move_ave() :
    global df_movav
    # priod   作成する期間
    # mov_ave_dd    何日間の移動平均か
    priod = 90
    mov_ave_dd = 7 
    df_movav = df.tail(priod+mov_ave_dd)
    df_movav['step'] = df_movav['step'].rolling(mov_ave_dd).mean()
    df_movav = df_movav.tail(priod)

#   週間移動平均ランキング
def rank_week(flg) :
    global df_rank_week
    mov_ave_dd = 7 
    df_rank_week = df.copy()
    df_rank_week['step'] = df_rank_week['step'].rolling(mov_ave_dd).mean()
    df_rank_week = df_rank_week.sort_values('step',ascending=False)
    i = 0
    for index , row in df_rank_week.head(20).iterrows() :
        i += 1
        if flg == 0 :
            if i >= 11 :
                break 
        if flg == 1 :
            if i <= 10 :
                continue 

        date_str = index.strftime('%Y/%m/%d')
        if index.date() == lastdate :      # 最終データなら赤字にする
            date_str = f'<span class=red>{date_str}</span>'
        out.write(f'<tr><td align="right">{i}</td><td>{row["step"]:5.0f}</td><td>{date_str}</td></tr>')

def rank_week_of_year(flg) :
    global df_rank_week
    mov_ave_dd = 7 
    df_rank_week = df.tail(365).copy()
    df_rank_week['step'] = df_rank_week['step'].rolling(mov_ave_dd).mean()
    df_rank_week = df_rank_week.sort_values('step',ascending=False)
    i = 0
    for index , row in df_rank_week.head(20).iterrows() :
        i += 1
        if flg == 0 :
            if i >= 11 :
                break 
        if flg == 1 :
            if i <= 10 :
                continue 

        date_str = index.strftime('%Y/%m/%d')
        if index.date() == lastdate :      # 最終データなら赤字にする
            date_str = f'<span class=red>{date_str}</span>'
        out.write(f'<tr><td align="right">{i}</td><td>{row["step"]:5.0f}</td><td>{date_str}</td></tr>')


def post_pixela() :
    if debug == 1 :
        return
    post_days = 7      #  最近の何日をpostするか
    limit = 7000       #  この歩数以下なら pixela では0とみなす

    headers = {}
    headers['X-USER-TOKEN'] = pixela_token
    df_tail7 = df.tail(post_days)
    for index,row in df_tail7.iterrows() :
        data = {}
        data['date'] = index.strftime('%Y%m%d')
        step = int(row.step)
        if step < limit :
            data['quantity'] = '0'
        else :
            data['quantity'] = str(row.step)
        response = requests.post(url=pixela_url, json=data, headers=headers,verify=False)


def ranking_all1():   #  1位から10位
    rank_common(allrank,0)

def ranking_all2():    #  11位から20位
    rank_common(allrank,1)

def ranking_month():   #  過去30日のランキング
    rank_common(monrank,0)

def ranking_year():   #  今年のランキング
    rank_common(yearrank,0)

def ranking_year2():   #  今年のランキング   11-20位
    rank_common(yearrank,1)

def rank_common(rankdata,flg) :
    #  flg ..  0  1-10位を表示   1  11-20位を表示
    i =0 
    for index, row in rankdata.iterrows():
        i = i+1 
        if flg == 0 :
            if i >= 11 :
                break 
        if flg == 1 :
            if i <= 10 :
                continue 

        date_str = index.strftime("%y/%m/%d (%a)")
        index_date_part = index.date()
        if index_date_part == lastdate :      # 最終データなら赤字にする
            date_str = f'<span class=red>{date_str}</span>'
        out.write(f'<tr><td align="right">{i}</td><td>{row["step"]}</td><td>{date_str}</td></tr>')

def month_graph() :
    for yymm,ave in zip(yymm_list,ave_list) :
        yy = yymm.year - 2000
        mm = yymm.month
        out.write(f"['{yy:02}/{mm:02}',{ave:5.0f}],") 

def year_graph():
    for yy in range(2021, end_year+1) :    # 2021年 ～ 2023年
        out.write(f"['{yy}',{yearinfo[yy]:5.0f}],") 

def quar_graph():
    for name,ave in zip(quar_name,quar_ave) :
        haxis_name = name.strftime("%y/%m")
        out.write(f"['{haxis_name}',{ave:5.0f}],") 

def daily_graph() :
    for ix,step  in zip(dailyindex,dailystep) :
        dd = ix.strftime('%d')
        out.write(f"['{dd}',{step:5.0f}],") 

def daily_movav() :
    for index,row  in df_movav.iterrows() :
        dd = index.strftime("%m/%d")
        out.write(f"['{dd}',{row['step']:5.0f}],") 

def daily_hist() :
    for ix,step  in zip(dailyindex,dailystep) :
        dd = ix.strftime('%d')
        out.write(f"['{dd}',{step:5.0f}],") 

def month_table():
    for yymm,monthinfo in statinfo.items() :
        out.write(f'<tr><td>{int(yymm/100)}/{yymm % 100:02} </td>')
        out.write(f'<td align="right"> {monthinfo["mean"]:5.0f}</td>')
        out.write(f'<td align="right"> {monthinfo["median"]:5.0f}</td>')
        out.write(f'<td align="right"> {monthinfo["std"]:5.0f}</td>')
        out.write(f'<td align="right"> {monthinfo["max"]:8d} ({monthinfo["maxdate"]})</td>')
        out.write(f'<td align="right"> {monthinfo["min"]:8d} ({monthinfo["mindate"]})</td>')
        out.write("</tr>")
    
    out.write(f'<tr><td>全体 </td>')
    out.write(f'<td align="right"> {allinfo["mean"]:5.0f}</td>')
    out.write(f'<td align="right"> {allinfo["median"]:5.0f}</td>')
    out.write(f'<td align="right"> {allinfo["std"]:5.0f}</td>')
    out.write(f'<td align="right"> {allinfo["max"]:8d} ({allinfo["maxdate"]}) </td>')
    out.write(f'<td align="right"> {allinfo["min"]:8d} ({allinfo["mindate"]}) </td>')
    out.write("</tr>")


def read_config() : 
    global ftp_host,ftp_user,ftp_pass,ftp_url,debug,datafile,pixela_url,pixela_token,debug
    if not os.path.isfile(conffile) :
        debug = 1 
        return

    conf = open(conffile,'r', encoding='utf-8')
    ftp_host = conf.readline().strip()
    ftp_user = conf.readline().strip()
    ftp_pass = conf.readline().strip()
    ftp_url = conf.readline().strip()
    datafile = conf.readline().strip()
    pixela_url = conf.readline().strip()
    pixela_token = conf.readline().strip()
    debug  = int(conf.readline().strip())
    conf.close()

def ftp_upload() : 
    if debug == 1 :
        return 
    with FTP_TLS(host=ftp_host, user=ftp_user, passwd=ftp_pass) as ftp:
        ftp.storbinary('STOR {}'.format(ftp_url), open(resultfile, 'rb'))

def date_settings():
    global  today_date,today_mm,today_dd,today_yy,today_datetime,today_hh

    today_datetime = datetime.datetime.today()   # datetime 型
    today_date = datetime.date.today()           # date 型
    today_mm = today_date.month
    today_dd = today_date.day
    today_yy = today_date.year
    today_hh = today_datetime.hour     #  現在の 時


def today(s):
    #d = datetime.datetime.today().strftime("%m/%d %H:%M")
    d = today_datetime.strftime("%m/%d %H:%M")
    s = s.replace("%today%",d)
    out.write(s)

def curdate(s) :
    d = f'{lastdate} {lasthh}時'
    s = s.replace("%lastdate%",d)
    out.write(s)

def parse_template() :
    global out 
    f = open(templatefile , 'r', encoding='utf-8')
    out = open(resultfile,'w' ,  encoding='utf-8')
    for line in f :
        if "%lastdate%" in line :
            curdate(line)
            continue
        if "%month_graph" in line :
            month_graph()
            continue
        if "%daily_graph" in line :
            daily_graph()
            continue
        if "%daily_hist" in line :
            daily_hist()
            continue
        if "%daily_movav" in line :
            daily_movav()
            continue
        if "%month_table" in line :
            month_table()
            continue
        if "%ranking_all1" in line :
            rank_common(allrank,0)
            continue
        if "%ranking_all2" in line :
            rank_common(allrank,1)
            continue
        if "%ranking_month" in line :
            rank_common(monrank,0)
            continue
        if "%ranking_year%" in line :
            rank_common(yearrank,0)
            continue
        if "%ranking_year2%" in line :
            rank_common(yearrank,1)
            continue
        if "%year_graph" in line :
            year_graph()
            continue
        if "%quar_graph" in line :
            quar_graph()
            continue
        if "%rank_week1%" in line :
            rank_week(0)
            continue
        if "%rank_week2%" in line :
            rank_week(1)
            continue
        if "%rank_week_of_year1%" in line :
            rank_week_of_year(0)
            continue
        if "%rank_week_of_year2%" in line :
            rank_week_of_year(1)
            continue
        if "%month_ave_top%" in line :
            month_ave_top()
            continue
        if "%month_median_top%" in line :
            month_median_top()
            continue
        if "%month_max_top%" in line :
            month_max_top()
            continue
        if "%month_min_top%" in line :
            month_min_top()
            continue
        if "%today%" in line :
            today(line)
            continue
        if "%version%" in line :
            s = line.replace("%version%",version)
            out.write(s)
            continue
        out.write(line)

    f.close()
    out.close()


# ----------------------------------------------------------
main_proc()

