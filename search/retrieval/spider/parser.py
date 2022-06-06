import re
import os
import asyncio
import aiofiles
from ruia import Spider, Item, TextField, Request
from pathlib import Path

# request = Request()
# request.request_config['DELAY'] = 2

STORAGE = "spider/bj_news/"

def get_people_urls(file="spider/www.people.com.cn/custom.txt", url_pattern="http://([\w\.]+)\.people.com.cn/n\d*/\d+/\d+/.+?\.html"):
    url_pattern = re.compile(url_pattern)
    urls = []
    with open(file, 'r', encoding='utf-8') as f:
        for line in f.readlines():
            url = re.search(url_pattern, line)
            if url.group(1) != "jl":
                urls.append(url[0])
    return urls

def extract_id(url):
    m = re.search(r'\w+://[\w\.]+/[\w\d]+/(\d+)/(\d+)/(.*?)\.html', url)
    return m[1]+ '-' + m[2] + '-' + m[3]

class PeopleNewsItem(Item):
    jtitle = TextField(css_select='#jtitle', default='')
    title = TextField(css_select='div.text_c > h1', default='')
    ftitle = TextField(css_select='#ftitle', default='')
    # author = TextField(css_select='.sou1',default='')
    bza = TextField(css_select='div.bza > p', default='')
    paragraphs = TextField(css_select='div.show_text > p', many=True, default='')
    editor = TextField(css_select='div.edit', default='')
    url = ''

class BJNewsItem(Item):
    title = TextField(css_select='#newstit', default='')
    paragraphs = TextField(css_select="div.rm_txt_con p", many=True, default='')
    editor = TextField(css_select='div.edit', default='')
    url = ''


class PeopleNewsSpider(Spider):
    # start_urls = ["http://bj.people.com.cn/n2/2022/0222/c82841-35143913.html"]
    start_urls = get_people_urls(file="spider/bj.people.com.cn/custom.txt", url_pattern="http://(bj)\.people.com.cn/n\d*/\d+/\d+/.+?\.html")
    async def parse(self, response):
        
        item = await BJNewsItem.get_item(html=await response.text())
        item.url = response.url
        yield item

    async def process_item(self, item: BJNewsItem):
        async with aiofiles.open(STORAGE + extract_id(item.url) + ".txt", 'w', encoding='utf-8') as fd:
            # await fd.write(item.jtitle + '\n' if item.jtitle else '')
            await fd.write(item.title + '\n' if item.title else '')
            # await fd.write(item.ftitle + '\n' if item.ftitle else '')
            # await fd.write(" ".join(item.author.split("&nbsp;")) + '\n')
            # await fd.write(item.bza + '\n' if item.bza else '')
            for i in range(len(item.paragraphs) - 1):
                await fd.write(item.paragraphs[i] + '\n')
            await fd.write(item.editor + '\n' if item.editor else '')

if __name__ == '__main__':
    if not Path(STORAGE).is_dir():
        Path(STORAGE).mkdir()
    
    PeopleNewsSpider.start()

    for file in os.listdir(STORAGE):
        filename = f'{STORAGE}/{file}'
        if os.path.getsize(filename) <= 1024:
            os.remove(filename)
