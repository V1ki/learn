import requests
import google.protobuf.text_format as text_format
import dm_pb2 as Danmaku
import pandas as pd

url = 'https://api.bilibili.com/x/v2/dm/web/seg.so'
# 861330037,925848891
params = {
    'type': 1,  # 弹幕类型
    'oid': 925848891,  # cid
    'pid': 861330037,  # avid
    'segment_index': 1  # 弹幕分段
}

df = pd.DataFrame()

for i in range(1, 10):
    params['segment_index'] = i
    resp = requests.get(url, params)
    data = resp.content
    danmaku_seg = Danmaku.DmSegMobileReply()
    danmaku_seg.ParseFromString(data)

    for ele in danmaku_seg.elems:
        print(text_format.MessageToString(ele, as_utf8=True, as_one_line=True))
        # 将 DanmakuElem 转换成 DataFrame
        item = {}
        item['id'] = ele.id
        item['progress'] = ele.progress
        item['mode'] = ele.mode
        item['fontsize'] = ele.fontsize
        item['color'] = ele.color
        item['midHash'] = ele.midHash
        item['content'] = ele.content
        item['ctime'] = ele.ctime
        item['weight'] = ele.weight
        item['attr'] = ele.attr
        item_df = pd.DataFrame(item, index=[0])
        df = pd.concat([df, item_df])


# print(text_format.MessageToString(danmaku_seg.elems[0], as_utf8=True))
df.to_csv('danmaku.csv', index=False)