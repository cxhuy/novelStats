# 모든 플랫폼이 갖고 있는 데이터
# id 작품 아이디
# title 작품 제목
# author 작가
# genre 장르 (로맨스, 로판, 판타지, 현판, 무협, 미스터리, 라이트노벨)
# time 크롤링 시작, 종료 시간
# keywords 제목 키워드
# chapters 회차수

# 몇몇 플랫폼과 공유하고 있는 데이터
# total_likes 전체 추천수

# 해당 플랫폼에 유니크한 데이터
# recent_views 최근화 조회

import requests, schedule, time, traceback
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from konlpy.tag import Hannanum, Okt

f = open("logs/navernovel/" + datetime.now().strftime("%Y%m%d%H%M%S") + ".txt", 'w')

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

# extracts number from string
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

# extracts keywords from title
def extractKeywords(title):
    keywords = []
    for noun in hannanum.nouns(title):
        if noun not in keywords: keywords.append(noun)
    for noun in okt.nouns(title):
        if noun not in keywords: keywords.append(noun)
    return keywords

# prints and writes toPrint
def printAndWrite(toPrint):
    print(toPrint)
    f.write('\n' + str(toPrint))
    f.flush()

# puts input novel on a waitlist to fetch end data later
def checkLater(novel):
    try:
        novelUrl = "https://novel.naver.com/challenge/list?novelId=" + str(novel["id"])
        currentTime = datetime.now()
        novelPage = getSoup(novelUrl)

        novel["end_rating"] = float(novelPage.find(class_="grade_area").select_one('em').text.strip())
        if (novel["chapters"] > 0) :
            novel["end_views"] = extractVal(novelPage.find(class_="list_type2").
                                            select('li')[0].find(class_="rating").find_all(class_="count")[-1].text)
        novel["end_comments"] = extractVal(novelPage.find(id="reviewCommentCnt").text)
        novel["end_likes"] = extractVal(novelPage.find(class_="info_book").find(id="concernCount").text)
        novel["end_time"] = currentTime
        printAndWrite(novel)

    except:
        printAndWrite("ERROR AT " + str(novel["id"]))
        printAndWrite(traceback.format_exc())

    return schedule.CancelJob

# refreshes every minute checking for newly uploaded novels
def printNewNovels():
    printAndWrite('\n' + str(datetime.now()) + "\n[New Novels]")
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
            novel["genre"] = "로맨스"

            # try crawling additional information from the novel's individual page
            novelUrl = "https://novel.naver.com/challenge/list?novelId=" + str(novel["id"])
            currentTime = datetime.now()

            try:
                novelPage = getSoup(novelUrl)

                # novel["start_favs"] = -1
                # novel["end_favs"] = -1

                novel["start_rating"] = float(novelPage.find(class_="grade_area").select_one('em').text.strip())
                novel["end_rating"] = -1

                novel["start_comments"] = extractVal(novelPage.find(id="reviewCommentCnt").text)
                novel["end_comments"] = -1

                novel["start_views"] = 0
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
                printAndWrite("ERROR AT " + str(novel["id"]))
                printAndWrite(traceback.format_exc())

        # if there were new novels, update last novel id to the most recently uploaded novel's id
        if (len(newNovels) > 0): lastNovelId = newNovels[0]["id"]

    if (len(newNovels) == 0):
        printAndWrite("none")

    else:
        for novelToPrint in newNovels:
            printAndWrite(novelToPrint)

    printAndWrite("\n[Old Novels]")


printAndWrite("started script at " + str(datetime.now()) + "\n")

# run function printNewNovels every minute
schedule.every().minute.at(":00").do(printNewNovels)

while True:
    schedule.run_pending()
    time.sleep(0.25)