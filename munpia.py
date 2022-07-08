# 모든 플랫폼이 갖고 있는 데이터
# id 작품 아이디
# title 작품 제목
# author 작가
# genre 장르 (무협, 판타지, 퓨전, 게임, 스포츠, 로맨스, 라이트노, 현대판타지, 대체역사, 전쟁*밀리터리, SF, 추리, 공포*미스테리, 일반소, 시*수필,
#            중*단편, 아동소설*동화, 드라마, 연극*시나리오, BL, 팬픽*패러디)
# time 크롤링 시작, 종료 시간
# keywords 제목 키워드
# chapters 회차수

# 몇몇 플랫폼과 공유하고 있는 데이터
# monopoly 독점 여부 (0 = 독점 아님, 1 = 선독점, 2 = 독점)
# total_likes 전체 추천수
# favs 선작수
# total_views 전체 조회수
# tags 태그

# 해당 플랫폼에 유니크한 데이터
# male, female 성별별 독자수
# age 연령별 독자 비율
# registration 작품 등록일
# latest_chapter 마지막 회차 업로드일
# characters 총 글자수

import requests, schedule, time, traceback
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from konlpy.tag import Hannanum, Okt

f = open("logs/munpia/" + datetime.now().strftime("%Y%m%d%H%M%S") + ".txt", 'w')

okt = Okt()
hannanum = Hannanum()

lastNovelId = [-1, -1, -1]
initialRun = [True, True, True]

# function for getting soup of input url
def getSoup(url):
    response = requests.get(url)
    assert response.status_code == 200, response.status_code
    html = response.text
    soup = BeautifulSoup(html, 'html.parser')
    return soup

# extracts numbers from string
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

# runs scrapPage functions for all pages
def scrapAllPages():
    printAndWrite('\n' + str(datetime.now()) + "\n[New Novels]")
    scrapPage("https://novel.munpia.com/page/novelous/group/nv.pro/gpage/1", 0)     # 무료 작가연재
    scrapPage("https://novel.munpia.com/page/novelous/group/nv.regular/gpage/1", 1) # 무료 일반연재
    scrapPage("https://novel.munpia.com/page/novelous/group/pl.serial/gpage/1", 2)  # 유료 연재작
    printAndWrite("\n[Old Novels]")

# puts input novel on a waitlist to fetch end data later
def checkLater(novel):
    try:
        novelUrl = "https://novel.munpia.com/" + str(novel["id"])
        currentTime = datetime.now()
        novelPage = getSoup(novelUrl)

        try:
            novelTagList = novelPage.find(class_="story-box").find(class_="tag-list").select('a')
            novelTags = []
            for tag in novelTagList:
                novelTags.append(tag.text.strip().replace('#', ''))
            novel["tags"] = novelTags

        except:
            novel["tags"] = []

        novelDetails = novelPage.find(class_="detail-box")
        novelPage = str(novelPage)
        novelPage = novelPage[novelPage.find("'남성', "):]

        novel["male"] = getScriptNumber(novelPage, novelPage.find("'남성', "))
        novel["female"] = getScriptNumber(novelPage, novelPage.find("'여성', "))
        novel["age_10"] = getScriptNumber(novelPage, novelPage.find("'10대', "))
        novel["age_20"] = getScriptNumber(novelPage, novelPage.find("'20대', "))
        novel["age_30"] = getScriptNumber(novelPage, novelPage.find("'30대', "))
        novel["age_40"] = getScriptNumber(novelPage, novelPage.find("'40대', "))
        novel["age_50"] = getScriptNumber(novelPage, novelPage.find("'50대 이상', "))

        novel["end_favs"] = extractVal(novelDetails.find(class_="trigger-subscribe").find('b').text)
        novelDetails = novelDetails.select('dl')[-1].select('dd')
        novel["end_total_views"] = extractVal(novelDetails[1].text)
        novel["end_likes"] = extractVal(novelDetails[2].text)
        novel["end_time"] = currentTime
        printAndWrite(novel)

    except:
        printAndWrite("ERROR AT " + str(novel["id"]))
        printAndWrite(traceback.format_exc())

    return schedule.CancelJob

# refreshes every minute checking for newly uploaded novels
def scrapPage(url, pricing):
    global lastNovelId, initialRun
    newNovels = []
    novelList = getSoup(url).find(id="SECTION-LIST").select('li')

    # if this is the first time running the script, don't fetch the novels but update the last novel id
    if (initialRun[pricing] == True):
        lastNovelId[pricing] = int(novelList[0].find(class_="title").get('href').split('https://novel.munpia.com/')[-1])
        initialRun[pricing] = False

    else:
        for i in range(len(novelList)):
            novel = {}
            currentNovel = novelList[i]
            novel["pricing"] = ["무료 작가연재", "무료 일반연재", "유료 연재작"][pricing]
            novel["id"] = int(currentNovel.find(class_="title").get('href').split('https://novel.munpia.com/')[-1])

            # if the current novel was already crawled before, break from loop
            if (novel["id"] == lastNovelId[pricing]): break

            novel["title"] = currentNovel.find(class_="title").text.strip()
            novel["author"] = currentNovel.find(class_="author").text.strip()

            # try crawling additional information from the novel's individual page
            novelUrl = 'https://novel.munpia.com/' + str(novel["id"])
            currentTime = datetime.now()

            try:
                novelDetails = getSoup(novelUrl).find(class_="detail-box")
                novel["genre"] = novelDetails.find(class_="meta-path").find('strong').text.replace(' ', '').split(',')

                try:
                    novel["monopoly"] = novelDetails.select_one('a').find('span').text.strip()

                except:
                    novel["monopoly"] = "비독점"

                novel["start_favs"] = extractVal(novelDetails.find(class_="trigger-subscribe").find('b').text)
                novel["end_favs"] = -1

                novelTime = novelDetails.select('dl')[-2].select('dd')

                novel["registration"] = datetime.strptime(novelTime[0].text, "%Y.%m.%d %H:%M")
                novel["latest_chapter"] = datetime.strptime(novelTime[1].text, "%Y.%m.%d %H:%M")

                if ((currentTime - novel["latest_chapter"]).total_seconds() > 120): continue

                novelDetails = novelDetails.select('dl')[-1].select('dd')

                novel["chapters"] = extractVal(novelDetails[0].text)
                novel["characters"] = extractVal(novelDetails[3].text)
                novel["start_total_views"] = extractVal(novelDetails[1].text)
                novel["end_total_views"] = -1
                novel["start_likes"] = extractVal(novelDetails[2].text)
                novel["end_likes"] = -1
                novel["start_time"] = currentTime
                novel["end_time"] = -1
                novel["male"] = -1
                novel["female"] = -1
                novel["age_10"] = -1
                novel["age_20"] = -1
                novel["age_30"] = -1
                novel["age_40"] = -1
                novel["age_50"] = -1
                novel["keywords"] = extractKeywords(novel["title"])

                newNovels.append(novel)

                # schedule checkLater function for this novel
                laterTime = currentTime + timedelta(minutes=10)
                laterTime = str(laterTime.hour).rjust(2, '0') + ':' + str(laterTime.minute).rjust(2, '0')
                schedule.every().day.at(laterTime).do(checkLater, novel)

            except:
                printAndWrite("ERROR AT " + str(novel["id"]))
                printAndWrite(traceback.format_exc())

        # if there were new novels, update last novel id to the most recently uploaded novel's id
        if (len(newNovels) > 0): lastNovelId[pricing] = newNovels[0]["id"]

    for novelToPrint in newNovels:
        printAndWrite(novelToPrint)

printAndWrite("started script at " + str(datetime.now()) + "\n")

# run function scrapAllPages every minute
schedule.every().minute.at(":00").do(scrapAllPages)

while True:
    schedule.run_pending()
    time.sleep(0.25)