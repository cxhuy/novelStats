import requests, schedule, time
from bs4 import BeautifulSoup
from datetime import datetime

url = 'https://novel.munpia.com/page/novelous/group/nv.regular/gpage/1'
lastNovelId = -1
initialRun = True

def getSoup(url):
    response = requests.get(url)

    assert response.status_code == 200, response.status_code

    html = response.text
    soup = BeautifulSoup(html, 'html.parser')

    return soup

def extractVal(dd):
    return int(dd.text.split(' ')[0].replace(',', ''))

def printNewNovels():
    print(datetime.now())
    global lastNovelId, initialRun
    newNovels = []
    novelList = getSoup(url).find(id="SECTION-LIST").select('li')

    if (initialRun == True):
        lastNovelId = int(novelList[0].find(class_="title").get('href').split('https://novel.munpia.com/')[-1])
        initialRun = False

    else:
        for i in range(len(novelList)):
            novel = {}
            currentNovel = novelList[i]
            novel["id"] = int(currentNovel.find(class_="title").get('href').split('https://novel.munpia.com/')[-1])
            if (novel["id"] == lastNovelId): break
            novel["title"] = currentNovel.find(class_="title").text.strip()
            novel["author"] = currentNovel.find(class_="author").text.strip()

            novelUrl = 'https://novel.munpia.com/' + str(novel["id"])
            novelDetails = getSoup(novelUrl).find(class_="detail-box").select('dl')[-1].select('dd')
            novel["chapters"] = extractVal(novelDetails[0])
            novel["letters"] = extractVal(novelDetails[3])
            novel["start_views"] = extractVal(novelDetails[1])
            novel["finish_views"] = -1
            novel["start_likes"] = extractVal(novelDetails[2])
            novel["finish_likes"] = -1
            # print("id: " + str(novel["id"]) + "\ntitle: " + novel["title"] + "\nauthor: " + novel["author"] + "\n")
            newNovels.append(novel)
        if (len(newNovels) > 0): lastNovelId = newNovels[0]["id"]

    # print(lastNovelId)
    if (len(newNovels) == 0):
        print("none\n")
    else:
        for novelToPrint in newNovels:
            print(novelToPrint)
        print()
    # print("none" if len(newNovels) == 0 else newNovels, end='\n')

print("started script at " + str(datetime.now()) + "\n")

schedule.every().minute.at(":00").do(printNewNovels)

while True:
    schedule.run_pending()
    time.sleep(0.25)