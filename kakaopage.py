import requests, schedule, time, traceback, json, os, pymysql, random
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from konlpy.tag import Hannanum, Okt
from dotenv import load_dotenv

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.0.0 Safari/537.36'
}

f = open("logs/kakaopage/" + datetime.now().strftime("%Y%m%d%H%M%S") + ".txt", 'w')

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

lastNovelId = [-1, -1, -1, -1, -1, -1, -1]
initialRun = [True, True, True, True, True, True, True]

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

# deletes double quotations in string
def deleteDQ(anno):
    toReturn = ""
    while(anno.find("\"description\"") != -1):
        start = anno.find("\"description\"") + 15
        end = anno[start:].find("\",\"") + start
        fixedString = anno[start:end].replace('\"', '')
        toReturn = ''.join([toReturn, anno[:start], fixedString, "\",\""])
        anno = anno[end + 3:]
    return toReturn + anno

# runs scrapPage functions for all pages
def scrapAllPages():
    printAndWrite('\n' + str(datetime.now()) + "\n[New Novels]")
    scrapPage("https://api2-page.kakao.com/api/v1/store/filter/search?category_uid=11&subcategory_uid=86&page=0", 0) # 판타지
    # time.sleep(2)
    scrapPage("https://api2-page.kakao.com/api/v1/store/filter/search?category_uid=11&subcategory_uid=120&page=0", 1) # 현판
    # time.sleep(2)
    scrapPage("https://api2-page.kakao.com/api/v1/store/filter/search?category_uid=11&subcategory_uid=89&page=0", 2) # 로맨스
    # time.sleep(2)
    scrapPage("https://api2-page.kakao.com/api/v1/store/filter/search?category_uid=11&subcategory_uid=117&page=0", 3) # 로판
    # time.sleep(2)
    scrapPage("https://api2-page.kakao.com/api/v1/store/filter/search?category_uid=11&subcategory_uid=87&page=0", 4) # 무협
    # time.sleep(2)
    # scrapPage("https://api2-page.kakao.com/api/v1/store/filter/search?category_uid=11&subcategory_uid=1113&page=0", 5) # 판드
    # scrapPage("https://api2-page.kakao.com/api/v1/store/filter/search?category_uid=11&subcategory_uid=1112&page=0", 6) # BL
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
        novelUrl = "https://page.kakao.com/home?seriesId=" + str(novel["novelId"])
        novelData = json.loads(getSoup(novelUrl).find(id="__NEXT_DATA__").text)["props"]["initialState"]["series"]["series"]

        novel["chapters"] = novelData["onSaleCount"]
        novel["end_total_views"] = novelData["readCount"]
        # novel["end_total_likes"] = novelData["like_count"]
        novel["end_total_comments"] = novelData["commentCount"]
        try:
            novel["monopoly"] = "독점" if novelData["servicePropertyList"][0]["name"] == "독점" else "비독점"
        except:
            novel["monopoly"] = "비독점"

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
        novelList = json.loads(deleteDQ(getSoup(url).text))["list"]

        # if this is the first time running the script, don't fetch the novels but update the last novel id
        if (initialRun[genre] == True):
            lastNovelId[genre] = novelList[0]["series_id"]
            initialRun[genre] = False

        else:
            scheduled_novels = []

            for job in schedule.jobs[1:]:
                scheduled_novels.append(job.job_func.args[0]["novelId"])

            for novelData in novelList:
                novel = {}

                novel["platform"] = "kakaopage"
                novel["genres"] = ["판타지", "현판", "로맨스", "로판", "무협", "판드", "BL"][genre]
                novel["novelId"] = novelData["series_id"]

                # if the current novel was already crawled before, break from loop
                if (novel["novelId"] == lastNovelId[genre] or novel["novelId"] in scheduled_novels): break

                currentTime = datetime.now()

                try:
                    novel["title"] = novelData["title"]
                    novel["author"] = novelData["author"]

                    novel["start_total_views"] = novelData["read_count"]
                    novel["end_total_views"] = -1

                    novel["chapters"] = -1

                    novel["start_total_likes"] = novelData["like_count"]
                    novel["end_total_likes"] = -1

                    novel["start_total_comments"] = novelData["comment_count"]
                    novel["end_total_comments"] = -1

                    novel["on_issue"] = True if novelData["on_issue"] == 'Y' else False

                    novel["monopoly"] = ""
                    novel["age_restriction"] = novelData["age_grade"]

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
        printAndWrite("Failed crawling kakaopage at " + str(datetime.now()) + "\n")
        printAndWrite(traceback.format_exc())

def startKakaopageCrawling():
    printAndWrite("started script at " + str(datetime.now()) + "\n")

    # run function scrapAllPages every minute
    schedule.every().minute.at(":00").do(scrapAllPages)

    while True:
        schedule.run_pending()
        time.sleep(0.25)