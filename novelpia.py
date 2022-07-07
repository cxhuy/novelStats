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
# exclusive 독점 여부 (0 = 독점 아님, 1 = 선독점, 2 = 독점)
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

f = open("logs/novelpia/" + datetime.now().strftime("%Y%m%d%H%M%S") + ".txt", 'w')

okt = Okt()
hannanum = Hannanum()

lastNovelId = [-1, -1]
initialRun = [True, True]

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.0.0 Safari/537.36'
}

# function for getting soup of input url
def getSoup(url):
    response = requests.get(url, headers=headers)
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
    scrapPage("https://novelpia.com/freestory/all/date/1/all/?main_genre=", 0) # 자유연재
    scrapPage("https://novelpia.com/plus/all/date/1/?main_genre=", 1)          # 플러스
    printAndWrite("\n[Old Novels]")

# puts input novel on a waitlist to fetch end data later
def checkLater(novel):
    try:
        novelUrl = 'https://novelpia.com/novel/' + str(novel["id"])
        currentTime = datetime.now()
        novelPage = getSoup(novelUrl)

        novel["end_favs"] = extractVal(novelPage.find(id="like_text").text)
        novel["end_alarm"] = extractVal(novelPage.find(id="alarm_text").text)
        novel["end_total_views"] = extractVal(novelPage.find_all(class_="more_info")[-1].select('span')[0].text.strip())
        novel["end_likes"] = extractVal(novelPage.find_all(class_="more_info")[-1].select('span')[2].text.strip())
        novel["end_time"] = currentTime

        recent_coins = 0
        total_coins = 0

        for coin in novelPage.find(id="donation_board1").select('tr')[:-1]:
            coin = coin.select('td')[-1].text
            if coin == "후원된 내역이 없습니다." or coin == "작가님의 첫 후원자가 되어주세요!": break
            recent_coins += extractVal(coin)

        for coin in novelPage.find(id="donation_board2").select('tr')[:-1]:
            coin = coin.select('td')[-1].text
            if coin == "후원된 내역이 없습니다." or coin == "작가님의 첫 후원자가 되어주세요!": break
            total_coins += extractVal(coin)

        novel["recent_coins"] = recent_coins
        novel["total_coins"] = total_coins

        printAndWrite(novel)

    except:
        printAndWrite("ERROR AT " + str(novel["id"]))
        printAndWrite(traceback.format_exc())

    return schedule.CancelJob

# refreshes every minute checking for newly uploaded novels
def scrapPage(url, price):
    global lastNovelId, initialRun
    newNovels = []
    novelList = getSoup(url).find_all(class_="novelbox")

    # if this is the first time running the script, don't fetch the novels but update the last novel id
    if (initialRun[price] == True):
        lastNovelId[price] = int(novelList[0].find(class_="name_st").get('onclick').split('/')[-1].replace('\';', ''))
        initialRun[price] = False

    else:
        for i in range(len(novelList)):
            novel = {}
            currentNovel = novelList[i]
            novel["price"] = price
            novel["id"] = int(currentNovel.find(class_="name_st").get('onclick').split('/')[-1].replace('\';', ''))

            # if the current novel was already crawled before, break from loop
            if (novel["id"] == lastNovelId[price]): break

            novel["title"] = currentNovel.find(class_="name_st").text.strip()
            novel["author"] = currentNovel.find(class_="info_font").text.strip()

            # try crawling additional information from the novel's individual page
            novelUrl = 'https://novelpia.com/novel/' + str(novel["id"])
            currentTime = datetime.now()

            try:
                novelPage = getSoup(novelUrl)

                genre = []
                tags = []

                for tag in novelPage.find_all(class_="more_info")[0].select('span')[2:-1]:
                    tag = tag.text.strip().replace('#', '')
                    if tag in ["판타지", "무협", "현대", "로맨스", "현대판타지", "라이트노벨", "공포", "SF", "스포츠", "대체역사", "기타", "패러디"]:
                        genre.append(tag)
                    else:
                        tags.append(tag)

                novel["genre"] = genre
                novel["tags"] = tags

                # 0 = 독점 아님, 1 = 독점
                novel["monopoly"] = 1 if novelPage.find(class_="b_mono") is not None else 0

                novel["age_restriction"] = "15" if novelPage.find(class_="b_15") is not None else "ALL"

                novel["start_favs"] = extractVal(novelPage.find(id="like_text").text)
                novel["end_favs"] = -1

                novel["start_alarm"] = extractVal(novelPage.find(id="alarm_text").text)
                novel["end_alarm"] = -1

                novel["chapters"] = extractVal(novelPage.find_all(class_="more_info")[-1].select('span')[1].text.strip())
                novel["start_total_views"] = extractVal(novelPage.find_all(class_="more_info")[-1].select('span')[0].text.strip())
                novel["end_total_views"] = -1
                novel["start_likes"] = extractVal(novelPage.find_all(class_="more_info")[-1].select('span')[2].text.strip())
                novel["end_likes"] = -1
                novel["start_time"] = currentTime
                novel["end_time"] = -1
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
        if (len(newNovels) > 0): lastNovelId[price] = newNovels[0]["id"]

    for novelToPrint in newNovels:
        printAndWrite(novelToPrint)

printAndWrite("started script at " + str(datetime.now()) + "\n")

# run function scrapAllPages every minute
schedule.every().minute.at(":00").do(scrapAllPages)

while True:
    schedule.run_pending()
    time.sleep(0.25)