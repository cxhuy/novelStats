import requests, schedule, time, traceback, json, os, pymysql, random
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from konlpy.tag import Hannanum, Okt
from dotenv import load_dotenv

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.0.0 Safari/537.36'
}

f = open("logs/kakaostage/" + datetime.now().strftime("%Y%m%d%H%M%S") + ".txt", 'w')

load_dotenv()

conn = pymysql.connect(
    host=os.environ.get('dbhost'),
    user=os.environ.get('dbuser'),
    password=os.environ.get('dbpassword'),
    db=os.environ.get('dbname'),
    charset='utf8'
)

cur = conn.cursor()

sql = "SET SESSION wait_timeout=43200;"
cur.execute(sql)
conn.commit()

okt = Okt()
hannanum = Hannanum()

lastNovelId = [-1, -1, -1, -1, -1, -1, -1]
initialRun = [True, True, True, True, True, True, True]

# function for getting soup of input url
def getSoup(url):
    response = requests.get(url, headers=headers)
    if (response.status_code != 200):
        printAndWrite(str(response.status_code) + "at kakaostage")
        return None
    else:
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
        return soup

# function for getting javascript rendered html of input url
# def getRenderedHtml(url):
#     session = HTMLSession()
#     response = session.get(url)
#
#     assert response.status_code == 200, response.status_code
#
#     response.html.render()
#
#     return response.html

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
        if noun not in keywords: keywords.append(conn.escape_string(noun))
    for noun in okt.nouns(title):
        if noun not in keywords: keywords.append(conn.escape_string(noun))
    return keywords

# prints and writes toPrint
def printAndWrite(toPrint):
    print(toPrint)
    f.write('\n' + str(toPrint))
    f.flush()

# runs scrapPage functions for all pages
def scrapAllPages():
    printAndWrite('\n' + str(datetime.now()) + "\n[Kakaostage New Novels]")
    scrapPage("https://api-pagestage.kakao.com/novels/genres/1?subGenreIds=1&page=0&size=20&sort=latestPublishedAt,desc&sort=id,desc&adult=false", 0) # 판타지
    scrapPage("https://api-pagestage.kakao.com/novels/genres/2?subGenreIds=2&page=0&size=20&sort=latestPublishedAt,desc&sort=id,desc&adult=false", 1) # 현판
    scrapPage("https://api-pagestage.kakao.com/novels/genres/3?subGenreIds=3&page=0&size=20&sort=latestPublishedAt,desc&sort=id,desc&adult=false", 2) # 무협
    scrapPage("https://api-pagestage.kakao.com/novels/genres/4?subGenreIds=4&page=0&size=20&sort=latestPublishedAt,desc&sort=id,desc&adult=false", 3) # 로맨스
    scrapPage("https://api-pagestage.kakao.com/novels/genres/5?subGenreIds=5&page=0&size=20&sort=latestPublishedAt,desc&sort=id,desc&adult=false", 4) # 로판
    scrapPage("https://api-pagestage.kakao.com/novels/genres/6?subGenreIds=6&page=0&size=20&sort=latestPublishedAt,desc&sort=id,desc&adult=false", 5) # BL
    scrapPage("https://api-pagestage.kakao.com/novels/genres/7?subGenreIds=7&page=0&size=20&sort=latestPublishedAt,desc&sort=id,desc&adult=false", 6) # 자유
    printAndWrite("\n[Kakaostage Old Novels]")

# store novel data in db
def storeNovel(novel):
    keys = list(novel.keys())
    novelDataKeys = list(filter(lambda key: key not in ['genres', 'keywords', 'tags'], keys))
    novelDataValues = list(
        map(lambda key: "\'" + conn.escape_string(novel[key]) + "\'" if type(novel[key]) == str else str(novel[key]), novelDataKeys))

    sql = "insert into novelData (" + ", ".join(novelDataKeys) + ") values (" + ", ".join(novelDataValues) + ")"

    cur.execute(sql)

    lastNovelInstanceId = cur.lastrowid

    if "tags" in novel:
        sql = "insert into tags (novelInstanceId, tag) values (" + str(lastNovelInstanceId) + ", %s)"
        cur.executemany(sql, novel["tags"])

    sql = "insert into keywords (novelInstanceId, keyword) values (" + str(lastNovelInstanceId) + ", %s)"
    cur.executemany(sql, novel["keywords"])

    sql = "insert into genres (novelInstanceId, genre) values (" + str(lastNovelInstanceId) + ", %s)"
    cur.executemany(sql, novel["genres"])

    conn.commit()

# puts input novel on a waitlist to fetch end data later
def checkLater(novel):
    try:
        novelUrl = 'https://api-pagestage.kakao.com/novels/' + str(novel["novelId"])
        novelData = json.loads(getSoup(novelUrl).text)

        novel["end_favs"] = novelData["favoriteCount"]
        novel["end_total_views"] = novelData["viewCount"]
        novel["end_total_likes"] = novelData["episodeLikeCount"]

        printAndWrite(novel)

    except:
        printAndWrite("ERROR AT " + str(novel["novelId"]))
        printAndWrite(traceback.format_exc())

    currentTime = datetime.now().strftime('%Y-%m-%d %H:%M:%S.000')
    novel["end_time"] = currentTime

    storeNovel(novel)
    time.sleep(random.uniform(0.1, 0.2))

    return schedule.CancelJob

# refreshes every minute checking for newly uploaded novels
def scrapPage(url, genre):
    try:
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
                novel["genres"] = [["판타지"], ["현판"], ["무협"], ["로맨스"], ["로판"], ["BL"], ["자유"]][genre]
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

                    # novel["avg_characters"] = novelData["avgBodySize"]
                    novel["total_characters"] = int(novelData["avgBodySize"] * novel["chapters"])

                    novel["start_total_likes"] = novelData["episodeLikeCount"]
                    novel["end_total_likes"] = -1

                    novel["registration"] = datetime.strptime(novelData["firstPublishedAt"], '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%d %H:%M:%S.000')

                    novel["stage_on"] = novelData["stageOn"]
                    novel["page_go"] = novelData["pageGo"]
                    novel["monopoly"] = "독점" if novelData["onlyStage"] else "비독점"
                    novel["age_restriction"] = 15 if novelData["ageRating"] == "FIFTEEN" else 0

                    novel["start_time"] = currentTime.strftime('%Y-%m-%d %H:%M:%S.000')
                    novel["end_time"] = -1

                    novel["keywords"] = extractKeywords(novel["title"])

                    newNovels.append(novel)

                    # schedule checkLater function for this novel
                    laterTime = currentTime + timedelta(minutes=int(os.environ.get('crawlingterm')))
                    laterTime = str(laterTime.hour).rjust(2, '0') + ':' + str(laterTime.minute).rjust(2, '0')
                    schedule.every().day.at(laterTime).do(checkLater, novel)

                except:
                    printAndWrite("ERROR AT " + str(novel["novelId"]))
                    printAndWrite(traceback.format_exc())

                time.sleep(random.uniform(0.1, 0.2))

            # if there were new novels, update last novel id to the most recently uploaded novel's id
            if (len(newNovels) > 0): lastNovelId[genre] = newNovels[0]["novelId"]

        for novelToPrint in newNovels:
            printAndWrite(novelToPrint)

    except:
        printAndWrite("Failed crawling kakaostage at " + str(datetime.now()) + "\n")
        printAndWrite(traceback.format_exc())

def startKakaostageCrawling():
    printAndWrite("started script at " + str(datetime.now()) + "\n")

    # run function scrapAllPages every minute
    schedule.every().minute.at(os.environ.get('crawlsat')).do(scrapAllPages)

    while True:
        schedule.run_pending()
        time.sleep(0.25)