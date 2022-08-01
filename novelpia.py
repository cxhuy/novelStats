import requests, schedule, time, traceback, os, pymysql, random
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from konlpy.tag import Hannanum, Okt
from dotenv import load_dotenv

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.0.0 Safari/537.36'
}

f = open("logs/novelpia/" + datetime.now().strftime("%Y%m%d%H%M%S") + ".txt", 'w')

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

lastNovelId = [-1, -1]
initialRun = [True, True]

# function for getting soup of input url
def getSoup(url):
    response = requests.get(url, headers=headers)
    if (response.status_code != 200):
        printAndWrite(str(response.status_code) + "at novelpia")
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
    printAndWrite('\n' + str(datetime.now()) + "\n[Novelpia New Novels]")
    scrapPage("https://novelpia.com/freestory/all/date/1/all/?main_genre=", 0) # 자유연재
    scrapPage("https://novelpia.com/plus/all/date/1/?main_genre=", 1)          # 플러스
    printAndWrite("\n[Novelpia Old Novels]")

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
        novelUrl = 'https://novelpia.com/novel/' + str(novel["novelId"])
        novelPage = getSoup(novelUrl)

        novel["end_favs"] = extractVal(novelPage.find(id="like_text").text)
        novel["end_alarm"] = extractVal(novelPage.find(id="alarm_text").text)
        novel["end_total_views"] = extractVal(novelPage.find_all(class_="more_info")[-1].select('span')[0].text.strip())
        novel["end_total_likes"] = extractVal(novelPage.find_all(class_="more_info")[-1].select('span')[2].text.strip())

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
        printAndWrite("ERROR AT " + str(novel["novelId"]))
        printAndWrite(traceback.format_exc())

    currentTime = datetime.now().strftime('%Y-%m-%d %H:%M:%S.000')
    novel["end_time"] = currentTime

    storeNovel(novel)
    time.sleep(random.uniform(0.1, 0.2))

    return schedule.CancelJob

# refreshes every minute checking for newly uploaded novels
def scrapPage(url, pricing):
    try:
        global lastNovelId, initialRun
        newNovels = []
        novelList = getSoup(url).find_all(class_="novelbox")

        # if this is the first time running the script, don't fetch the novels but update the last novel id
        if (initialRun[pricing] == True):
            lastNovelId[pricing] = int(novelList[0].find(class_="name_st").get('onclick').split('/')[-1].replace('\';', ''))
            initialRun[pricing] = False

        else:
            scheduled_novels = []

            for job in schedule.jobs[1:]:
                scheduled_novels.append(job.job_func.args[0]["novelId"])

            for i in range(len(novelList)):
                novel = {}
                currentNovel = novelList[i]
                novel["platform"] = "novelpia"
                novel["pricing"] = ["자유연재", "플러스"][pricing]
                novel["novelId"] = int(currentNovel.find(class_="name_st").get('onclick').split('/')[-1].replace('\';', ''))

                # if the current novel was already crawled before, break from loop
                if (novel["novelId"] == lastNovelId[pricing] or novel["novelId"] in scheduled_novels): break

                novel["title"] = currentNovel.find(class_="name_st").text.strip()
                novel["author"] = currentNovel.find(class_="info_font").text.strip()

                # try crawling additional information from the novel's individual page
                novelUrl = 'https://novelpia.com/novel/' + str(novel["novelId"])
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
                            tags.append(conn.escape_string(tag))

                    novel["genres"] = genre
                    novel["tags"] = tags

                    novel["monopoly"] = "독점" if novelPage.find(class_="b_mono") is not None else "비독점"

                    novel["age_restriction"] = 15 if novelPage.find(class_="b_15") is not None else 0

                    novel["start_favs"] = extractVal(novelPage.find(id="like_text").text)
                    novel["end_favs"] = -1

                    novel["start_alarm"] = extractVal(novelPage.find(id="alarm_text").text)
                    novel["end_alarm"] = -1

                    novel["chapters"] = extractVal(novelPage.find_all(class_="more_info")[-1].select('span')[1].text.strip())
                    novel["start_total_views"] = extractVal(novelPage.find_all(class_="more_info")[-1].select('span')[0].text.strip())
                    novel["end_total_views"] = -1
                    novel["start_total_likes"] = extractVal(novelPage.find_all(class_="more_info")[-1].select('span')[2].text.strip())
                    novel["end_total_likes"] = -1
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
            if (len(newNovels) > 0): lastNovelId[pricing] = newNovels[0]["novelId"]

        for novelToPrint in newNovels:
            printAndWrite(novelToPrint)

    except:
        printAndWrite("Failed crawling novelpia at " + str(datetime.now()) + "\n")
        printAndWrite(traceback.format_exc())

def startNovelpiaCrawling():
    printAndWrite("started script at " + str(datetime.now()) + "\n")

    # run function scrapAllPages every minute
    schedule.every().minute.at(os.environ.get('crawlsat')).do(scrapAllPages)

    while True:
        schedule.run_pending()
        time.sleep(0.25)