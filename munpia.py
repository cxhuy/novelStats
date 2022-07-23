import requests, schedule, time, traceback, os, pymysql, random
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from konlpy.tag import Hannanum, Okt
from dotenv import load_dotenv

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.0.0 Safari/537.36'
}

f = open("logs/munpia/" + datetime.now().strftime("%Y%m%d%H%M%S") + ".txt", 'w')

load_dotenv()

conn = pymysql.connect(
    host=os.environ.get('dbhost'),
    user=os.environ.get('dbuser'),
    password=os.environ.get('dbpassword'),
    db=os.environ.get('dbname'),
    charset='utf8'
)

cur = conn.cursor()

sql = "SET SESSION wait_timeout=28800;"
cur.execute(sql)
conn.commit()

okt = Okt()
hannanum = Hannanum()

lastNovelId = [-1, -1, -1]
initialRun = [True, True, True]

# function for getting soup of input url
def getSoup(url):
    response = requests.get(url, headers=headers)
    if (response.status_code != 200):
        printAndWrite(str(response.status_code) + "at munpia")
        return None
    else:
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
    printAndWrite('\n' + str(datetime.now()) + "\n[Munpia New Novels]")
    scrapPage("https://novel.munpia.com/page/novelous/group/nv.pro/gpage/1", 0)     # 무료 작가연재
    scrapPage("https://novel.munpia.com/page/novelous/group/nv.regular/gpage/1", 1) # 무료 일반연재
    scrapPage("https://novel.munpia.com/page/novelous/group/pl.serial/gpage/1", 2)  # 유료 연재작
    printAndWrite("\n[Munpia Old Novels]")

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
        novelUrl = "https://novel.munpia.com/" + str(novel["novelId"])
        novelPage = getSoup(novelUrl)

        try:
            novelTagList = novelPage.find(class_="story-box").find(class_="tag-list").select('a')
            novelTags = []
            for tag in novelTagList:
                novelTags.append(conn.escape_string(tag.text.strip().replace('#', '')))
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
        novel["end_total_likes"] = extractVal(novelDetails[2].text)
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
def scrapPage(url, pricing):
    try:
        global lastNovelId, initialRun
        newNovels = []
        novelList = getSoup(url).find(id="SECTION-LIST").select('li')

        # if this is the first time running the script, don't fetch the novels but update the last novel id
        if (initialRun[pricing] == True):
            lastNovelId[pricing] = int(novelList[0].find(class_="title").get('href').split('https://novel.munpia.com/')[-1])
            initialRun[pricing] = False

        else:
            scheduled_novels = []

            for job in schedule.jobs[1:]:
                scheduled_novels.append(job.job_func.args[0]["novelId"])

            for i in range(len(novelList)):
                novel = {}
                currentNovel = novelList[i]
                novel["platform"] = "munpia"
                novel["pricing"] = ["무료 작가연재", "무료 일반연재", "유료 연재작"][pricing]
                novel["novelId"] = int(currentNovel.find(class_="title").get('href').split('https://novel.munpia.com/')[-1])

                # if the current novel was already crawled before, break from loop
                if (novel["novelId"] == lastNovelId[pricing] or novel["novelId"] in scheduled_novels): break

                novel["title"] = currentNovel.find(class_="title").text.strip()
                novel["author"] = currentNovel.find(class_="author").text.strip()

                # try crawling additional information from the novel's individual page
                novelUrl = 'https://novel.munpia.com/' + str(novel["novelId"])
                currentTime = datetime.now()

                try:
                    novelDetails = getSoup(novelUrl).find(class_="detail-box")
                    novel["genres"] = novelDetails.find(class_="meta-path").find('strong').text.replace(' ', '').split(',')

                    try:
                        novel["monopoly"] = novelDetails.select_one('a').find('span').text.strip()

                    except:
                        novel["monopoly"] = "비독점"

                    novel["start_favs"] = extractVal(novelDetails.find(class_="trigger-subscribe").find('b').text)
                    novel["end_favs"] = -1

                    novelTime = novelDetails.select('dl')[-2].select('dd')

                    novel["registration"] = datetime.strptime(novelTime[0].text, "%Y.%m.%d %H:%M").strftime('%Y-%m-%d %H:%M:%S.000')
                    # novel["latest_chapter"] = datetime.strptime(novelTime[1].text, "%Y.%m.%d %H:%M")
                    #
                    # if ((currentTime - novel["latest_chapter"]).total_seconds() > 120): continue

                    novelDetails = novelDetails.select('dl')[-1].select('dd')

                    novel["chapters"] = extractVal(novelDetails[0].text)
                    novel["total_characters"] = extractVal(novelDetails[3].text)
                    # novel["avg_characters"] = float(novel["characters"] / novel["chapters"])
                    novel["start_total_views"] = extractVal(novelDetails[1].text)
                    novel["end_total_views"] = -1
                    novel["start_total_likes"] = extractVal(novelDetails[2].text)
                    novel["end_total_likes"] = -1
                    novel["start_time"] = currentTime.strftime('%Y-%m-%d %H:%M:%S.000')
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
                    laterTime = currentTime + timedelta(minutes=os.environ.get('crawlingterm'))
                    laterTime = str(laterTime.hour).rjust(2, '0') + ':' + str(laterTime.minute).rjust(2, '0')
                    schedule.every().day.at(laterTime).do(checkLater, novel)

                except:
                    printAndWrite("ERROR AT " + str(novel["novelId"]))
                    printAndWrite(traceback.format_exc())

                time.sleep(random.uniform(0.1, 0.5))

            # if there were new novels, update last novel id to the most recently uploaded novel's id
            if (len(newNovels) > 0): lastNovelId[pricing] = newNovels[0]["novelId"]

        for novelToPrint in newNovels:
            printAndWrite(novelToPrint)

    except:
        printAndWrite("Failed crawling munpia at " + str(datetime.now()) + "\n")
        printAndWrite(traceback.format_exc())

def startMunpiaCrawling():
    printAndWrite("started script at " + str(datetime.now()) + "\n")

    # run function scrapAllPages every minute
    schedule.every().minute.at(":00").do(scrapAllPages)

    while True:
        schedule.run_pending()
        time.sleep(0.25)