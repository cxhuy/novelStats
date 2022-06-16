import requests
from bs4 import BeautifulSoup
from datetime import datetime

url = 'https://novel.munpia.com/page/novelous/group/nv.regular/genre/fantasy/gpage/1'
finishedMinute = -1
newNovels = []

def getSoup(url):
    response = requests.get(url)

    assert response.status_code == 200, response.status_code

    html = response.text
    soup = BeautifulSoup(html, 'html.parser')

    return soup

print("start script at " + str(datetime.now()) + "\n")

while(True):
    currentTime = datetime.now()
    if(currentTime.minute != finishedMinute and currentTime.second == 0):
        finishedMinute = currentTime.minute
        print(currentTime)

        novelList = getSoup(url).find(id="SECTION-LIST").select('li')

        for i in range(len(novelList)):
            novel = {}
            currentNovel = novelList[i]
            novel["id"] = int(currentNovel.find(class_="title").get('href').split('https://novel.munpia.com/')[-1])
            novel["author"] = currentNovel.find(class_="author").text.strip()
            novel["title"] = currentNovel.find(class_="title").text.strip()
            print("id: " + str(novel["id"]) + "\nauthor: " + novel["author"] + "\ntitle: " + novel["title"] + "\n")
            newNovels.append(novel)

        print(newNovels)