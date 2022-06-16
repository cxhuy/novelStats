import requests
from bs4 import BeautifulSoup
from datetime import datetime

url = 'https://novel.munpia.com/page/novelous/group/nv.regular/genre/fantasy/gpage/1'
finishedMinute = -1
print(datetime.now().second)

def getSoup(url):
    response = requests.get(url)

    assert response.status_code == 200, response.status_code

    html = response.text
    soup = BeautifulSoup(html, 'html.parser')

    return soup

while(True):
    currentTime = datetime.now()
    if(currentTime.minute != finishedMinute and currentTime.second == 0):
        finishedMinute = currentTime.minute
        print(currentTime)

        novelList = getSoup(url).find(id="SECTION-LIST").select('li')

        for i in range(len(novelList)):
            currentNovel = novelList[i]
            author = currentNovel.find(class_="author").text.strip()
            title = currentNovel.find(class_="title").text.strip()
            id = currentNovel.find(class_="title").get('href').split('https://novel.munpia.com/')[-1]
            print('author: ' + author + '\ntitle: ' + title + '\nid: ' + id + '\n')