import requests, schedule, time
from bs4 import BeautifulSoup
from datetime import datetime

url = 'https://novel.munpia.com/page/novelous/group/nv.regular/gpage/1'
lastNovelId = -1

def getSoup(url):
    response = requests.get(url)

    assert response.status_code == 200, response.status_code

    html = response.text
    soup = BeautifulSoup(html, 'html.parser')

    return soup

def printNewNovels():
    print(datetime.now())
    global lastNovelId
    newNovels = []
    novelList = getSoup(url).find(id="SECTION-LIST").select('li')

    for i in range(len(novelList)):
        novel = {}
        currentNovel = novelList[i]
        novel["id"] = int(currentNovel.find(class_="title").get('href').split('https://novel.munpia.com/')[-1])
        if (novel["id"] == lastNovelId): break
        novel["title"] = currentNovel.find(class_="title").text.strip()
        novel["author"] = currentNovel.find(class_="author").text.strip()
        print("id: " + str(novel["id"]) + "\ntitle: " + novel["title"] + "\nauthor: " + novel["author"] + "\n")
        newNovels.append(novel)
    if (len(newNovels) > 0): lastNovelId = newNovels[0]["id"]
    print(lastNovelId)
    print(newNovels)

print("started script at " + str(datetime.now()) + "\n")

schedule.every().minute.at(":00").do(printNewNovels)

while True:
    schedule.run_pending()
    time.sleep(0.25)