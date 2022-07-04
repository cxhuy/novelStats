import requests, schedule, time, traceback
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from konlpy.tag import Hannanum, Okt

okt = Okt()
hannanum = Hannanum()

url = 'https://novel.naver.com/challenge/genre?genre=101'
lastNovelId = -1
initialRun = True

# function for getting soup of input url
def getSoup(url):
    response = requests.get(url)
    assert response.status_code == 200, response.status_code
    html = response.text
    soup = BeautifulSoup(html, 'html.parser')
    return soup

# extract integer from string
def extractVal(val):
    return int(''.join([s for s in val if s.isdigit()]))

# gets number from script in page source string
def getScriptNumber(script, idx):
    scriptNumber = ""

    while (script[idx] != ','):
        idx += 1
    idx += 2

    while (script[idx] != ']' and script[idx] != ','):
        scriptNumber += script[idx]
        idx += 1

    return int(scriptNumber)

def extractKeywords(title):
    keywords = []
    for noun in hannanum.nouns(title):
        if noun not in keywords: keywords.append(noun)
    for noun in okt.nouns(title):
        if noun not in keywords: keywords.append(noun)
    return keywords

# puts input novel on a waitlist to fetch end data later
def checkLater(novel):
    try:
        novelUrl = "https://novel.naver.com/challenge/list?novelId=" + str(novel["id"])
        currentTime = datetime.now()
        novelPage = getSoup(novelUrl)

        novel["end_views"] = extractVal(novelPage.find(class_="list_type2").
                                        select('li')[0].find(class_="rating").find_all(class_="count")[-1].text)
        novel["end_comments"] = extractVal(novelPage.find(id="reviewCommentCnt").text)
        novel["end_likes"] = extractVal(novelPage.find(class_="info_book").find(id="concernCount").text)
        novel["end_time"] = currentTime
        print(novel)

    except:
        print("ERROR AT " + str(novel["id"]))
        traceback.print_exc()

    return schedule.CancelJob

# refreshes every minute checking for newly uploaded novels
def printNewNovels():
    print('\n' + str(datetime.now()) + "\n[New Novels]")
    global lastNovelId, initialRun
    newNovels = []
    novelList = getSoup(url).find(class_="list_type1").select('li')

    # if this is the first time running the script, don't fetch the novels but update the last novel id
    if (initialRun == True):
        lastNovelId = int(novelList[0].select_one('a').get('href').split("/best/list?novelId=")[-1])
        initialRun = False

    else:
        for i in range(len(novelList)):
            novel = {}
            currentNovel = novelList[i]
            novel["id"] = int(currentNovel.select_one('a').get('href').split("/best/list?novelId=")[-1])

            # if the current novel was already crawled before, break from loop
            if (novel["id"] == lastNovelId): break

            novel["title"] = currentNovel.select_one('a').get('title').strip()
            novel["author"] = currentNovel.find(class_="ellipsis").text.strip()
            novel["chapters"] = extractVal(currentNovel.find(class_="num_total").text.strip())
            novel["rating"] = float(currentNovel.find(class_="rating").select_one('em').text.strip())
            novel["genre"] = "로맨스"

            # try crawling additional information from the novel's individual page
            novelUrl = "https://novel.naver.com/challenge/list?novelId=" + str(novel["id"])
            currentTime = datetime.now()

            try:
                novelPage = getSoup(novelUrl)

                # novel["start_favs"] = -1
                # novel["end_favs"] = -1

                novel["start_comments"] = extractVal(novelPage.find(id="reviewCommentCnt").text)
                novel["end_comments"] = -1

                novel["start_views"] = -1
                novel["end_views"] = -1
                novel["start_likes"] = extractVal(novelPage.find(class_="info_book").find(id="concernCount").text)
                novel["end_likes"] = -1
                novel["start_time"] = currentTime
                novel["end_time"] = -1
                novel["keywords"] = extractKeywords(novel["title"])

                newNovels.append(novel)

                # schedule checkLater function for this novel
                laterTime = currentTime + timedelta(minutes=3)
                laterTime = str(laterTime.hour).rjust(2, '0') + ':' + str(laterTime.minute).rjust(2, '0')
                schedule.every().day.at(laterTime).do(checkLater, novel)

            except:
                print("ERROR AT " + str(novel["id"]))
                traceback.print_exc()

        # if there were new novels, update last novel id to the most recently uploaded novel's id
        if (len(newNovels) > 0): lastNovelId = newNovels[0]["id"]

    if (len(newNovels) == 0):
        print("none")

    else:
        for novelToPrint in newNovels:
            print(novelToPrint)

    print("\n[Old Novels]")


print("started script at " + str(datetime.now()) + "\n")

# run function printNewNovels every minute
schedule.every().minute.at(":00").do(printNewNovels)

while True:
    schedule.run_pending()
    time.sleep(0.25)