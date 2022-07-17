import requests, schedule, time, traceback, os, pymysql, random
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from konlpy.tag import Hannanum, Okt
from dotenv import load_dotenv

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.0.0 Safari/537.36'
}

f = open("logs/navernovel/" + datetime.now().strftime("%Y%m%d%H%M%S") + ".txt", 'w')

load_dotenv()

conn = pymysql.connect(
    host=os.environ.get('dbhost'),
    user=os.environ.get('dbuser'),
    password=os.environ.get('dbpassword'),
    db=os.environ.get('dbname'),
    charset='utf8'
)

cur = conn.cursor()

okt = Okt()
hannanum = Hannanum()

# url = 'https://novel.naver.com/challenge/genre?genre=101'
lastNovelId = [-1, -1, -1, -1, -1, -1, -1]
initialRun = [True, True, True, True, True, True, True]

# function for getting soup of input url
def getSoup(url):
    response = requests.get(url, headers=headers)
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
    printAndWrite('\n' + str(datetime.now()) + "\n[New Novels]")
    scrapPage("https://novel.naver.com/best/genre?genre=101", 0) # 로맨스
    scrapPage("https://novel.naver.com/best/genre?genre=109", 1) # 로판
    scrapPage("https://novel.naver.com/best/genre?genre=102", 2) # 판타지
    scrapPage("https://novel.naver.com/best/genre?genre=110", 3) # 현판
    scrapPage("https://novel.naver.com/best/genre?genre=103", 4) # 무협
    scrapPage("https://novel.naver.com/best/genre?genre=104", 5) # 미스터리
    scrapPage("https://novel.naver.com/best/genre?genre=106", 6) # 라이트노벨
    printAndWrite("\n[Old Novels]")

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
        novelUrl = "https://novel.naver.com/best/list?novelId=" + str(novel["novelId"])
        novelPage = getSoup(novelUrl)

        novel["end_rating"] = float(novelPage.find(class_="grade_area").select_one('em').text.strip())
        if (novel["chapters"] > 0) :
            novel["end_recent_views"] = extractVal(novelPage.find(class_="list_type2").select('li')[0].
                                            find(class_="rating").find_all(class_="count")[-1].text)
            novel["end_recent_comments"] = extractVal(novelPage.find(class_="list_type2").select('li')[0].
                                            find(class_="rating").find_all(class_="count")[0].find(class_="num").text)

        novel["end_reviews"] = extractVal(novelPage.find(id="reviewCommentCnt").text)
        novel["end_total_likes"] = extractVal(novelPage.find(class_="info_book").find(id="concernCount").text)
        printAndWrite(novel)

    except:
        printAndWrite("ERROR AT " + str(novel["novelId"]))
        printAndWrite(traceback.format_exc())

    currentTime = datetime.now().strftime('%Y-%m-%d %H:%M:%S.000')
    novel["end_time"] = currentTime

    storeNovel(novel)
    time.sleep(random.uniform(0.1, 0.5))

    return schedule.CancelJob

# refreshes every minute checking for newly uploaded novels
def scrapPage(url, genre):
    try:
        global lastNovelId, initialRun
        newNovels = []
        novelList = getSoup(url).find(class_="list_type1").select('li')

        # if this is the first time running the script, don't fetch the novels but update the last novel id
        if (initialRun[genre] == True):
            lastNovelId[genre] = int(novelList[0].select_one('a').get('href').split("/best/list?novelId=")[-1])
            initialRun[genre] = False

        else:
            scheduled_novels = []

            for job in schedule.jobs[1:]:
                scheduled_novels.append(job.job_func.args[0]["novelId"])

            for i in range(len(novelList)):
                novel = {}
                currentNovel = novelList[i]
                novel["platform"] = "navernovel"
                novel["novelId"] = int(currentNovel.select_one('a').get('href').split("/best/list?novelId=")[-1])

                # if the current novel was already crawled before, break from loop
                if (novel["novelId"] == lastNovelId[genre] or novel["novelId"] in scheduled_novels): break

                novel["title"] = currentNovel.select_one('a').get('title').strip()
                novel["author"] = currentNovel.find(class_="ellipsis").text.strip()
                novel["chapters"] = extractVal(currentNovel.find(class_="num_total").text.strip())
                novel["genres"] = ["로맨스", "로판", "판타지", "현판", "무협", "미스터리", "라이트노벨"][genre]

                # try crawling additional information from the novel's individual page
                novelUrl = "https://novel.naver.com/challenge/list?novelId=" + str(novel["novelId"])
                currentTime = datetime.now()

                try:
                    novelPage = getSoup(novelUrl)

                    # novel["start_favs"] = -1
                    # novel["end_favs"] = -1

                    novel["start_rating"] = float(novelPage.find(class_="grade_area").select_one('em').text.strip())
                    novel["end_rating"] = -1

                    novel["start_recent_comments"] = 0
                    novel["end_recent_comments"] = -1

                    novel["start_reviews"] = extractVal(novelPage.find(id="reviewCommentCnt").text)
                    novel["end_reviews"] = -1

                    novel["start_recent_views"] = 0
                    novel["end_recent_views"] = -1

                    novel["start_total_likes"] = extractVal(novelPage.find(class_="info_book").find(id="concernCount").text)
                    novel["end_total_likes"] = -1

                    novel["start_time"] = currentTime.strftime('%Y-%m-%d %H:%M:%S.000')
                    novel["end_time"] = -1

                    novel["keywords"] = extractKeywords(novel["title"])

                    newNovels.append(novel)

                    # schedule checkLater function for this novel
                    laterTime = currentTime + timedelta(minutes=70)
                    laterTime = str(laterTime.hour).rjust(2, '0') + ':' + str(laterTime.minute).rjust(2, '0')
                    schedule.every().day.at(laterTime).do(checkLater, novel)

                except:
                    printAndWrite("ERROR AT " + str(novel["novelId"]))
                    printAndWrite(traceback.format_exc())

                time.sleep(random.uniform(0.1, 0.5))

            # if there were new novels, update last novel id to the most recently uploaded novel's id
            if (len(newNovels) > 0): lastNovelId[genre] = newNovels[0]["novelId"]

        for novelToPrint in newNovels:
            printAndWrite(novelToPrint)

    except:
        printAndWrite("Failed crawling navernovel at " + str(datetime.now()) + "\n")
        printAndWrite(traceback.format_exc())

def startNavernovelCrawling():
    printAndWrite("started script at " + str(datetime.now()) + "\n")

    # run function printNewNovels every minute
    schedule.every().minute.at(":00").do(scrapAllPages)

    while True:
        schedule.run_pending()
        time.sleep(0.25)