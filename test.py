import re
import requests
import time
import csv
import json
from jsonpath import jsonpath
from requests.adapters import HTTPAdapter

# 服务器传输url与响应头
new_url = 'http://106.15.37.226/internal/insert'  # 新增舆情传输链接
update_url = 'http://106.15.37.226/internal/update'  # 更新舆情传输链接
get_url = 'http://106.15.37.226/internal/userDictionary'  # 专属敏感词获取链接
s_headers = {'key': 'f281088c8f10c1042610310854090cc22c742'}  # 服务器请求头

# 主词条搜索：云南、热门信息;页数限制;地点字典
l_keywords = ['云南', '玉溪', '普洱']
l_page = 30
page = 1
locations = {'云南': [{'昆明': 530100},
                    {'曲靖': 530300},
                    {'玉溪': 530400},
                    {'保山': 530500},
                    {'昭通': 530600},
                    {'丽江': 530700},
                    {'普洱': 530800},
                    {'临沧': 530900},
                    {'楚雄': 532300},
                    {'红河': 532500},
                    {'文山': 532600},
                    {'西双版纳': 532800},
                    {'大理': 532900},
                    {'德宏': 533100},
                    {'怒江': 533300},
                    {'迪庆': 533400},
                    ]}  # 一级地名字典
locations_2nd = [{'昆明': [{'五华': 530102}, {'盘龙': 530103}, {'西山': 530112}, {'官渡': 530111}, {'东川': 530113}, {'呈贡': 530114},
                         {'晋宁': 530115}, {'安宁': 530181}, {'富民': 530124}, {'嵩明': 530127}, {'禄劝': 530128}, {'寻甸': 530129},
                         {'石林': 530126}]},
                 {'曲靖': [{'麒麟': 530302}, {'沾益': 530303}, {'马龙': 530304}, {'宣威': 530381}, {'陆良': 530322}, {'师宗': 530323},
                         {'罗平': 530324}, {'富源': 530325},
                         {'会泽': 530326}]},
                 {'玉溪': [{'红塔': 530402}, {'江川': 530403}, {'澄江': 530481}, {'通海': 530423}, {'华宁': 530424}, {'易门': 530425},
                         {'峨山': 530426}, {'新平': 530427},
                         {'元江': 530428}]},
                 {'丽江': [{'古城区': 530702}, {'玉龙': 530721}, {'永胜': 530722}, {'华坪': 530723}, {'宁蒗': 530724}]},
                 {'普洱': [{'思茅': 530802}, {'宁洱': 530821}, {'澜沧': 530828}, {'墨江': 530822}, {'景谷': 530824}, {'西盟': 530829},
                         {'镇沅': 530825}, {'江城': 530826},
                         {'孟连': 530827}]},
                 {'保山': [{'隆阳': 530502}, {'腾冲': 530581}, {'施甸': 530521}, {'龙陵': 530523}, {'昌宁': 530524}]},
                 {'昭通': [{'昭阳': 530602}, {'水富': 530681}, {'鲁甸': 530621}, {'巧家': 530622}, {'盐津': 530623}, {'大关': 530624},
                         {'永善': 530625}, {'绥江': 530626},
                         {'镇雄': 530627}, {'彝良': 530628}, {'威信': 530629}]},
                 {'临沧': [{'凌翔': 530902}, {'凤庆': 530921}, {'云县': 530922}, {'永德': 530923}, {'镇康': 530924}, {'双江': 530925},
                         {'耿马': 530926},
                         {'沧源': 530927}]},
                 {'楚雄': [{'楚雄': 532301}, {'禄丰': 532302}, {'双柏': 532322}, {'牟定': 532323}, {'南华': 532324}, {'姚安': 532325},
                         {'大姚': 532326}, {'永仁': 532327},
                         {'元谋': 532328}, {'武定': 532329}]},
                 {'红河': [{'个旧': 532501}, {'开远': 532502}, {'蒙自': 532503}, {'弥勒': 532504}, {'屏边': 532523}, {'建水': 532524},
                         {'石屏': 532525}, {'泸西': 532527},
                         {'元阳': 532528}, {'红河': 532529}, {'金平': 532530}, {'绿春': 532531},
                         {'河口': 532532}]},
                 {'文山': [{'文山': 532601}, {'砚山': 532622}, {'西畴': 532623}, {'麻栗坡': 532624}, {'马关': 532625},
                         {'丘北': 532626}, {'广南': 532627},
                         {'富宁': 532628}]},
                 {'西双版纳': [{'景洪': 532801}, {'勐海': 532822}, {'勐腊': 532823}]},
                 {'大理': [{'大理': 532901}, {'漾濞': 532922}, {'祥云': 532923}, {'宾川': 532924}, {'弥渡': 532925}, {'南涧': 532926},
                         {'巍山': 532927}, {'永平': 532928},
                         {'云龙': 532929}, {'洱源': 532930}, {'剑川': 532931}, {'鹤庆': 532932}]},
                 {'怒江': [{'泸水': 533301}, {'福贡': 533323}, {'贡山': 533324}, {'兰坪': 533325}]},
                 {'德宏': [{'瑞丽': 533102}, {'芒市': 533103}, {'梁河': 533122}, {'盈江': 533123}, {'泷川': 533124}]},
                 {'迪庆': [{'香格里拉': 533401}, {'德钦': 533422}, {'维西': 533423}]}, ]  # 二级地名字典
data_dic = {'title': '',  # 热点
            'region': 530000,  # 地区代码 默认云南省
            'briefIntroduction': '',  # 摘要
            'heat': 0,  # 浏览量
            'posts': 0,  # 评论量
            'stars': 0,  # 星级
            'trend': '',  # 24小时趋势
            'url': '',  # 源链接
            # 'detail': '',  # 详情
            'status': 'new',  # 存在状态
            'source': '',
            }  # 有效数据字典

# 敏感词列表、链接列表
sup_sensitive_lists = []  # 专属敏感词列表
cleaned_query_list = []  # tag暂存列表
default_sensitive_words = []  # 默认敏感词列表
register_list = []  # 一轮之后的缓存tag列表：用于判断
last_register_list = []  # 多次更新后的缓存tag列表：用于对舆情是更新舆情还是新舆情作出判断
temp_list = []  # 暂存tag列表：用于判断tag在这轮更新中是否被调用过
url_list = []  # 暂存链接列表
result_query_list = [[], [], [], [], [], [], [], []]  # 多次暂存结果列表

sleep_time = 6  # 休眠时间设置
search = False  # 判断tag状态
c_count = 0  # 结果暂存计数
succeed_count = 0  # tag通过数
result_query_count = 8  # 结果暂存次数

dr = re.compile(r'<[^>]+>', re.S)  # 页面符号除去正则
dk = re.compile(r'<a {2}href="(.*?)" data-hide="">')  # 链接符号除去正则

# 会话超时重载
session = requests.Session()
session.mount('http://', HTTPAdapter(max_retries=3))
error_point = 0  # 会话异常次数


def catch_url(c_url, c_headers, c_params=None):
    """获取网页以及获取失败后的重载操作"""
    i = 0
    while i < 3:
        try:
            if c_params is None:
                html = requests.get(c_url, headers=c_headers, timeout=5)
                return html
            else:
                html = requests.get(c_url, headers=c_headers, params=c_params, timeout=5)
                return html
        except requests.exceptions.RequestException as e:
            i += 1
            print(e)
    return False


def post_url(c_url, c_headers, c_params=None):
    """获取网页以及获取失败后的重载操作"""
    i = 0
    while i < 3:
        try:
            if c_params is None:
                html = requests.post(c_url, headers=c_headers, timeout=5)
                return html
            else:
                html = requests.post(c_url, headers=c_headers, data=c_params, timeout=5)
                return html
        except requests.exceptions.RequestException as e:
            i += 1
            print(e)
    return False


def complex_message(s_list):
    """缓存信息合并"""
    return_list = []
    for times_list in s_list:
        for i in times_list:
            if i not in return_list:
                return_list.append(i)
    return return_list


def collection_message(string):
    """判断事件关联性"""
    if '云南' not in string:
        return False
    return True


def current_storage(s_list, r_list, count):
    """循环缓存机制"""
    count = count % result_query_count + 1
    s_list[count - 1] = r_list[:]
    print(s_list)


def existential_status(query):
    """判断话题存在状态"""
    status = 'update'
    if query not in last_register_list:
        status = 'new'
        if query not in register_list:
            register_list.append(query)
    # print(register_list)
    return status


def get_detail(con):
    """获取tag的detail信息"""
    d = jsonpath(con, f'$.data.cards[0]...mblog.text')
    if not d:
        d = jsonpath(con, f'$.data.cards[1]...mblog.text')
    if not d:
        return 'null'
    return d


def get_sup_sensitive():
    """获取服务器端专属敏感词"""
    sup_list = []
    resp = requests.get(get_url, headers=s_headers)
    rep = resp.json()
    # print(rep)
    if resp:
        # print(rep)
        p = list(jsonpath(rep, '$.data'))[0]
        # print(p)
        for i in p:
            sup_list.append(i)
        print(sup_list)
    return sup_list
    # p格式为{'username':'Erevan','dictionary':'1,2,3',}<String> <List[str]>


def get_trend(page):
    j_list = []
    if jsonpath(page, '$.data.read[0].time'):
        j_time = jsonpath(page, '$.data.read[0].time')
        j_list.append(j_time)
        for i in range(0, 24):
            i_list = jsonpath(page, f'$.data.read[{i}].value')
            for j in i_list:
                j_list.append(j)
    return j_list


def init_lib(dic_list):
    """初始化敏感词库、查询暂存库"""
    # choose = input('是否初始化查询库？(y/n)\n')
    choose = 'n'
    if choose == 'y':
        with open('l_query_list.csv', mode='w', newline=''):
            print('暂存链接文件已初始化！')
    else:
        with open('l_query_list.csv', mode='r', newline='') as f:
            csv_reader = csv.reader(f)
            for row in csv_reader:
                last_register_list.append(row[0])
            print('暂存文件已读入！')
            print(last_register_list)
            print(f'已从缓存文件中获取到{len(last_register_list)}条舆情')
    with open('敏感词库表统计.csv', mode='r', encoding='utf-8') as sen_d:
        csv_reader = csv.reader(sen_d)
        for row in csv_reader:
            dic_list.append(row[2])
    print('已完成词库整理!')


def location_get(string):
    """获取详情内容所在地"""
    def_loc = 530000
    for loc in locations.values():
        for i in loc:
            if list(i.keys())[0] in string:
                return list(i.values())[0]
    for loc_c in locations_2nd:
        for loc_k, loc_2 in loc_c.items():
            for j in loc_2:
                # print(j)
                if list(j.keys())[0] in string:
                    return list(j.values())[0]
    return def_loc


def message_recode():
    """敏感信息录入函数"""
    # print(q)
    status = existential_status(q)
    data_dic['title'] = title
    if bI:
        data_dic['briefIntroduction'] = bI
    else:
        data_dic['briefIntroduction'] = '具体信息见详情页'
    print(detail)
    data_dic['region'] = p_code
    data_dic['heat'] = heat
    data_dic['posts'] = mention
    data_dic['stars'] = int(stars)
    data_dic['trend'] = json.dumps(trend)
    # print(time)
    print(search)
    data_dic['status'] = status
    data_dic['url'] = url_rebuild(q)
    data_dic['source'] = '新浪微博'
    print(data_dic)
    print(data_dic['status'])
    # 传输分类
    return_choose(data_dic)


def new_post(data_dict):
    """新数据传输函数"""
    new_data = {'title': data_dict['title'],
                'region': data_dict['region'],
                'briefIntroduction': data_dict['briefIntroduction'],
                'heat': data_dict['heat'],
                'posts': data_dict['posts'],
                'stars': data_dict['stars'],
                'trend': data_dict['trend'],
                'url': data_dict['url'],
                'source': data_dict['source'],
                'user': None,
                }
    if data_dict['user']:
        new_data['user'] = data_dict['user']
    print(new_data)
    result = post_url(new_url, c_headers=s_headers, c_params=new_data)
    print(result.json())
    if result.status_code == 200:
        print('已传输一个新数据信息')
    else:
        print('新信息传输错误！请检查表单格式')


def return_choose(dic):
    """信息状态分流"""
    if dic['status'] == 'new':
        new_post(dic)
    elif dic['status'] == 'update':
        update_post(dic)
    else:
        print('未检索到该词条下状态信息！')


def screen_message(string, dic_list):
    """敏感信息筛选函数"""
    for word in dic_list:
        if word in string:
            print(f'the word:{word}')
            return True
    return False


def select_q(query):
    """筛选tag是否存在"""
    if query not in temp_list:
        temp_list.append(query)
        return True


def tag_data(query):
    """获取tag内热度信息"""
    url = f'https://m.s.weibo.com/ajax_topic/detail?q={query}'
    resp = catch_url(url, headers)
    data = json.loads(resp.text)
    # print(json.dumps(data, indent=True))
    return data


def trend_data(query):
    """获取tag趋势信息"""
    url = f'https://m.s.weibo.com/ajax_topic/trend?q={query}'
    resp = catch_url(url, headers)
    data = json.loads(resp.text)
    # print(json.dumps(data, indent=True))
    return data


def update_post(data_dict):
    """更新数据传输函数"""
    update_data = {'title': data_dict['title'],
                   'heat': data_dict['heat'],
                   'posts': data_dict['posts'],
                   'stars': data_dict['stars'],
                   'trend': data_dict['trend'],
                   'source': data_dict['source'],
                   'user': None,
                   }
    if data_dict['user']:
        update_data['user'] = data_dict['user']
    print(update_data)
    result = post_url(update_url, c_headers=s_headers, c_params=update_data)
    print(result.json())
    if result.status_code == 200:
        print('已传输一个更新数据信息')
    else:
        print('更新信息传输错误！请检查表单格式')


def url_rebuild(query):
    """详细页链接"""
    _url = 'https://s.weibo.com/weibo?q='
    _url = _url + query
    return _url


def url_split(array):
    """标签提取函数"""
    q_list = []
    for url in array:
        sp = re.compile(r'http.*?containerid=.*?q%3D(?P<url_splited>.*?)&', re.S)
        q = sp.findall(url)
        if q:
            q_list = q_list + q
    # print(q_list)
    # print(len(q_list))
    return q_list


def visited_url(query, e_point):
    """获取tag评论信息"""
    params = {
        "containerid": "100103type=1&q={}".format(query),
        "page_type": "searchall",
        "page": 1
    }
    resp = 200
    rep = ''
    try:
        resp = catch_url(main_url, headers, params)
        if resp:
            rep = json.loads(resp.text)
        else:
            rep = 'Read time out.'
            # print(rep)
            return rep
    except requests.exceptions.RequestException as e:
        e_point += 1
        print(e)
    resp_temp = resp
    return rep


while True:
    for l_keyword in l_keywords:
        print(f"开始爬取{l_keyword}地区舆情")
        while page <= l_page:
            """拿到链接与主要信息"""
            headers = {
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                              'Chrome/103.0.5060.53 Safari/537.36 ',
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,"
                          "application/signed-exchange;v=b3;q=0.9",
                "accept-encoding": "gzip, deflate, br"
            }  # 请求头信息
            params = {
                "containerid": "100103type=1&q={}".format(l_keyword),
                "page_type": "searchall",
                "page": page
            }
            main_url = 'https://m.weibo.cn/api/container/getIndex'
            r = catch_url(main_url, headers, params)
            # print(r.status_code)
            # print(r.json())
            if r:
                cards = r.json()["data"]["cards"]
                # print(cards)
                if cards:
                    text_list = jsonpath(cards, '$..mblog.text')
                    # print('text_list is:')
                    # print(text_list)
                    for i in text_list:
                        df = dk.findall(i)
                        for j in df:
                            url_list.append(j)
                    print(f'已完成{page}页！')
            else:
                print(f'第{page}页连接超时...')
            page += 1
        page = 1
    print(f'\n已爬取到{len(url_list)}条热门信息')

    # print(url_list)
    qlist = url_split(url_list)
    init_lib(default_sensitive_words)
    sup_sensitive_lists = get_sup_sensitive()

    count = 0

    for q in qlist:
        print('——————')
        # print(q)
        if select_q(q):
            # 页面一获取数据
            content = visited_url(q, error_point)
            # data = tag_data(q)
            title = ''
            bI = ''
            detail = ''
            if jsonpath(content, '$.data.cardlistInfo.cardlist_title') and jsonpath(content,
                                                                                    '$.data.cardlistInfo.desc'):
                title = list(jsonpath(content, '$.data.cardlistInfo.cardlist_title'))[0].strip('#')
                bI = list(jsonpath(content, '$.data.cardlistInfo.desc'))[0]
                detail = dr.sub('', list(get_detail(content))[0])
            else:
                print("HTTPSConnectionPool(host='m.s.weibo.com', port=443): loss of data. (content lost)")
            # 页面二获取数据
            data_con = tag_data(q)
            if jsonpath(data_con, '$.data.baseInfo.count.read'):
                heat = list(jsonpath(data_con, '$.data.baseInfo.count.read'))[0]
                mention = list(jsonpath(data_con, '$.data.baseInfo.count.mention'))[0]
                stars = list(jsonpath(data_con, '$.data.baseInfo.count.star'))[0]
            else:
                print("HTTPSConnectionPool(host='m.s.weibo.com', port=443): loss of data. (data lost)")

            # 页面三获取数据
            data_trend = trend_data(q)
            # time = list(list(get_trend(data_trend)[0]))[0]
            trend = list(get_trend(data_trend))[1:]

            # 加工后数据
            p_code = location_get(detail)
            print(title)
            for sup_dic in sup_sensitive_lists:
                # 专属词查询
                user_n = sup_dic.get('username', 'fall to get!')
                list_l = sup_dic.get('dictionary', 'fall to get!')
                # print(list_l)
                # print(user_n)
                sup_sensitive_words = list_l.split(",")
                # print(sup_sensitive_words)
                if screen_message(detail, sup_sensitive_words):
                    search = True
                    data_dic['user'] = user_n
                else:
                    search = False
                # if True:
                if collection_message(detail) and search:
                    message_recode()
                    succeed_count += 1
            if not search:
                # 调用默认词库
                print('无专属词库词语匹配，正在调用默认词源...')
                search = screen_message(detail, default_sensitive_words)
                data_dic['user'] = False
                if collection_message(detail) and search:
                    message_recode()
                    succeed_count += 1
            else:
                print('未满足筛选要求！')
        else:
            print('出现重复信息！')
        print(f"__count__:{count + 1}")
        count += 1
    print(register_list)
    print(last_register_list)
    temp_register_list = register_list + last_register_list
    with open('l_query_list.csv', mode='w', newline='') as file:
        csv_writer = csv.writer(file)
        for i in temp_register_list:
            csv_writer.writerow([i])
    print(f'本次运行共传输{succeed_count}条舆情...')

    # 舆情更替判定
    current_storage(result_query_list, temp_register_list, c_count)
    c_count += 1

    # 初始化缓存数据
    print(f'等待时间:{sleep_time}s...\n')
    time.sleep(sleep_time)
    print('等待结束，开始初始化缓存数据')
    page = 1
    succeed_count = 0
    temp_list = []
    url_list = []
    register_list = []
    last_register_list = []
    temp_register_list = []
    print('已初始化缓存数据')
