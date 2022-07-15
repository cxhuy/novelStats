import requests, schedule, time, traceback, json
from requests_html import HTMLSession
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from konlpy.tag import Hannanum, Okt

f = open("logs/kakaostage/" + datetime.now().strftime("%Y%m%d%H%M%S") + ".txt", 'w')

okt = Okt()
hannanum = Hannanum()

lastNovelId = [-1, -1, -1, -1, -1, -1, -1]
initialRun = [True, True, True, True, True, True, True]

# function for getting soup of input url
def getSoup(url):
    response = requests.get(url)
    assert response.status_code == 200, response.status_code
    html = response.text
    soup = BeautifulSoup(html, 'html.parser')
    return soup

# function for getting javascript rendered html of input url
def getRenderedHtml(url):
    session = HTMLSession()
    response = session.get(url)

    assert response.status_code == 200, response.status_code

    response.html.render()

    return response.html

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
    scrapPage("https://api-pagestage.kakao.com/novels/genres/1?subGenreIds=1&page=0&size=20&sort=latestPublishedAt,desc&sort=id,desc&adult=false", 0) # 판타지
    scrapPage("https://api-pagestage.kakao.com/novels/genres/2?subGenreIds=2&page=0&size=20&sort=latestPublishedAt,desc&sort=id,desc&adult=false", 1) # 현판
    scrapPage("https://api-pagestage.kakao.com/novels/genres/3?subGenreIds=3&page=0&size=20&sort=latestPublishedAt,desc&sort=id,desc&adult=false", 2) # 무협
    scrapPage("https://api-pagestage.kakao.com/novels/genres/4?subGenreIds=4&page=0&size=20&sort=latestPublishedAt,desc&sort=id,desc&adult=false", 3) # 로맨스
    scrapPage("https://api-pagestage.kakao.com/novels/genres/5?subGenreIds=5&page=0&size=20&sort=latestPublishedAt,desc&sort=id,desc&adult=false", 4) # 로판
    scrapPage("https://api-pagestage.kakao.com/novels/genres/6?subGenreIds=6&page=0&size=20&sort=latestPublishedAt,desc&sort=id,desc&adult=false", 5) # BL
    scrapPage("https://api-pagestage.kakao.com/novels/genres/7?subGenreIds=7&page=0&size=20&sort=latestPublishedAt,desc&sort=id,desc&adult=false", 6) # 자유
    printAndWrite("\n[Old Novels]")

# puts input novel on a waitlist to fetch end data later
def checkLater(novel):
    try:
        novelUrl = 'https://api-pagestage.kakao.com/novels/' + str(novel["novelId"])
        currentTime = datetime.now().strftime('%Y-%m-%d %H:%M:%S.000')
        novelData = json.loads(getSoup(novelUrl).text)

        novel["end_favs"] = novelData["favoriteCount"]
        novel["end_total_views"] = novelData["viewCount"]
        novel["end_total_likes"] = novelData["episodeLikeCount"]
        novel["end_time"] = currentTime

        printAndWrite(novel)

    except:
        printAndWrite("ERROR AT " + str(novel["novelId"]))
        printAndWrite(traceback.format_exc())

    return schedule.CancelJob

# refreshes every minute checking for newly uploaded novels
def scrapPage(url, genre):
    global lastNovelId, initialRun
    newNovels = []
    novelList = json.loads(getSoup(url).text)["content"]

    # if this is the first time running the script, don't fetch the novels but update the last novel id
    if (initialRun[genre] == True):
        lastNovelId[genre] = novelList[0]["stageSeriesNumber"]
        initialRun[genre] = False

    else:
        scheduled_novels = []

        for job in schedule.jobs[1:]:
            scheduled_novels.append(job.job_func.args[0]["novelId"])

        for novelData in novelList:
            novel = {}

            novel["platform"] = "kakaostage"
            novel["genres"] = ["판타지", "현판", "무협", "로맨스", "로판", "BL", "자유"][genre]
            novel["novelId"] = novelData["stageSeriesNumber"]

            # if the current novel was already crawled before, break from loop
            if (novel["novelId"] == lastNovelId[genre] or novel["novelId"] in scheduled_novels): break

            currentTime = datetime.now()

            try:
                novel["title"] = novelData["title"]
                novel["author"] = novelData["nickname"]["name"]

                novel["start_favs"] = novelData["favoriteCount"]
                novel["end_favs"] = -1

                novel["start_total_views"] = novelData["viewCount"]
                novel["end_total_views"] = -1

                novel["chapters"] = novelData["publishedEpisodeCount"]

                novel["avg_characters"] = novelData["avgBodySize"]
                novel["total_characters"] = int(novelData["avgBodySize"] * novelData["chapters"])

                novel["start_total_likes"] = novelData["episodeLikeCount"]
                novel["end_total_likes"] = -1

                novel["registration"] = datetime.strptime(novelData["firstPublishedAt"], '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%d %H:%M:%S.000')

                novel["stageOn"] = novelData["stageOn"]
                novel["pageGo"] = novelData["pageGo"]
                novel["monopoly"] = "독점" if novelData["onlyStage"] else "비독점"
                novel["age_restriction"] = "15" if novelData["ageRating"] == "FIFTEEN" else "ALL"

                novel["start_time"] = currentTime.strftime('%Y-%m-%d %H:%M:%S.000')
                novel["end_time"] = -1

                novel["keywords"] = extractKeywords(novel["title"])

                newNovels.append(novel)

                # schedule checkLater function for this novel
                laterTime = currentTime + timedelta(hours=1)
                laterTime = str(laterTime.hour).rjust(2, '0') + ':' + str(laterTime.minute).rjust(2, '0')
                schedule.every().day.at(laterTime).do(checkLater, novel)

            except:
                printAndWrite("ERROR AT " + str(novel["novelId"]))
                printAndWrite(traceback.format_exc())

        # if there were new novels, update last novel id to the most recently uploaded novel's id
        if (len(newNovels) > 0): lastNovelId[genre] = newNovels[0]["novelId"]

    for novelToPrint in newNovels:
        printAndWrite(novelToPrint)

printAndWrite("started script at " + str(datetime.now()) + "\n")

# run function scrapAllPages every minute
schedule.every().minute.at(":00").do(scrapAllPages)

while True:
    schedule.run_pending()
    time.sleep(0.25)