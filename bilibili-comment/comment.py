from typing import Dict, Any, Tuple

import requests
import pandas as pd
from time import sleep


def bv2av(bv: str) -> tuple[str, str]:
    _data = requests.get(f'https://api.bilibili.com/x/web-interface/view?bvid={bv}').json()
    return (_data['data']['aid'], _data['data']['cid'])


# 从 cookie 文件中读取 cookie
cookie_str = open('cookie.txt', 'r').read()


def bilibili_request_get(url: str, bv: str, cookie: str = cookie_str):
    payload = {}
    headers = {
        'authority': 'api.bilibili.com',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'cache-control': 'no-cache',
        'cookie': cookie,
        'origin': 'https://www.bilibili.com',
        'pragma': 'no-cache',
        'referer': f'https://www.bilibili.com/video/{bv}',
        'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/108.0.0.0 Safari/537.36 '
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    return response.json()


def get_replies(bv: str, av: str, n: int = 0, cookie: str = cookie_str):
    # https://github.com/SocialSisterYi/bilibili-API-collect/blob/master/comment/list.md#%E8%8E%B7%E5%8F%96%E8%AF%84%E8%AE%BA%E5%8C%BA%E6%98%8E%E7%BB%86_%E7%BF%BB%E9%A1%B5%E5%8A%A0%E8%BD%BD
    url = f"https://api.bilibili.com/x/v2/reply/main?mode=3&next={n}&oid={av}&type=1"
    return bilibili_request_get(url, bv, cookie)


def get_sub_replies(bv: str, av: str, root: str, n: int = 0, cookie: str = cookie_str):
    # https://github.com/SocialSisterYi/bilibili-API-collect/blob/master/comment/list.md#%E8%8E%B7%E5%8F%96%E6%8C%87%E5%AE%9A%E8%AF%84%E8%AE%BA%E7%9A%84%E5%9B%9E%E5%A4%8D
    url = f"https://api.bilibili.com/x/v2/reply/reply?root={root}&oid={av}&type=1&pn={n}"
    return bilibili_request_get(url, bv, cookie)


def convert_reply2item(bv_id: str, cid: str, r: dict) -> dict:
    member = r['member']
    uname = member['uname']
    avatar = member['avatar']
    sex = member['sex']
    mid = member['mid']
    level = member['level_info']['current_level']
    rcount = r['rcount']
    count = r['count']
    like = r['like']
    ctime = r['ctime']
    content = r['content']['message']
    parent = r['parent']

    return {
        'rpid': r['rpid'],
        'uname': uname,
        'avatar': avatar,
        'sex': sex,
        'mid': mid,
        'level': level,
        'rcount': rcount,
        'count': count,
        'like': like,
        'ctime': ctime,
        'content': content,
        'parent': parent,
        'bv': bv_id,
        'aid': r['oid'],
        'cid': cid
    }


# http://api.bilibili.com/x/v2/dm/web/seg.so?type=1&oid=861330037&segment_index=1
def save_reply2dataframe(item: dict, d: pd.DataFrame) -> pd.DataFrame:
    # 检查 d 中是否已经有了这个评论
    if 'rpid' in d and item['rpid'] in d['rpid'].values:
        return d
    # pd.concat
    return pd.concat([d, pd.DataFrame(item, index=[0])])


def save_sub_replies(row: dict, d: pd.DataFrame) -> pd.DataFrame:
    rcount = row['rcount']
    if d[d['parent'] == row['rpid']].shape[0] == rcount:
        return d
    # 获取评论的回复
    sleep(1)
    _datas = get_sub_replies(row['bv'], row['oid'], row['rpid'])

    _data = _datas["data"]
    if _datas['code'] != 0 or _data is None:
        print(f"no data for {row} -- datas: {_datas}")
        return d
    page = _data["page"]
    page_count = page["count"]
    count = 0
    while count < page_count:
        _data = _datas["data"]
        page = _data["page"]
        page_num = page["num"]
        for r in _data["replies"]:
            item = convert_reply2item(bv_id, row['cid'], r)
            d = save_reply2dataframe(item, d)
            count += 1
        sleep(1)
        c = d[d['parent'] == row['rpid']].shape[0]
        print(
            f"start fetch {row['rpid']} page {page_num + 1} current:{c} all_count:{page['count']} ")
        _datas = get_sub_replies(row['bv'], row['oid'], row['rpid'], page_num + 1)
    return d


if __name__ == '__main__':
    bv_id = 'BV1JV4y1A7NZ'
    aid, cid = bv2av(bv_id)

    df = pd.DataFrame()
    datas = get_replies(bv_id, aid)
    data = datas["data"]
    cursor = data["cursor"]

    while not cursor['is_end']:
        data = datas["data"]
        cursor = data["cursor"]
        for r in data["replies"]:
            item = convert_reply2item(bv_id, cid, r)
            df = save_reply2dataframe(item, df)
            if r['rcount'] > 0:
                df = save_sub_replies(item, df)
        n = cursor['next']
        print(
            f"start fetch page {n} current:{df.shape[0]} all_count:{cursor['all_count']}  -- is_end: {cursor['is_end']}")
        datas = get_replies(bv_id, aid, n)

    df.to_csv('comments.csv', index=False)
