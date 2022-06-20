import requests, schedule, time
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

url = 'https://novel.munpia.com/page/novelous/group/nv.regular/gpage/1'
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
    return int(val.text.split(' ')[0].replace(',', ''))

# puts input novel on a waitlist to fetch end data later
def checkLater(novel):
    try:
        novelUrl = 'https://novel.munpia.com/' + str(novel["id"])
        currentTime = datetime.now()
        novelDetails = getSoup(novelUrl).find(class_="detail-box")
        novel["end_favs"] = extractVal(novelDetails.find(class_="trigger-subscribe").find('b'))
        novelDetails = novelDetails.select('dl')[-1].select('dd')
        novel["end_views"] = extractVal(novelDetails[1])
        novel["end_likes"] = extractVal(novelDetails[2])
        novel["end_time"] = currentTime
        print(novel)

    except Exception as e:
        print("ERROR AT " + str(novel["id"]))
        print(e)

    return schedule.CancelJob

# refreshes every minute checking for newly uploaded novels
def printNewNovels():
    print('\n' + str(datetime.now()) + "\n[New Novels]")
    global lastNovelId, initialRun
    newNovels = []
    novelList = getSoup(url).find(id="SECTION-LIST").select('li')

    # if this is the first time running the script, don't fetch the novels but update the last novel id
    if (initialRun == True):
        lastNovelId = int(novelList[0].find(class_="title").get('href').split('https://novel.munpia.com/')[-1])
        initialRun = False

    else:
        for i in range(len(novelList)):
            novel = {}
            currentNovel = novelList[i]
            novel["id"] = int(currentNovel.find(class_="title").get('href').split('https://novel.munpia.com/')[-1])

            # if the current novel was already crawled before, break from loop
            if (novel["id"] == lastNovelId): break

            novel["title"] = currentNovel.find(class_="title").text.strip()
            novel["author"] = currentNovel.find(class_="author").text.strip()

            # try crawling additional information from the novel's individual page
            novelUrl = 'https://novel.munpia.com/' + str(novel["id"])
            currentTime = datetime.now()

            try:
                novelDetails = getSoup(novelUrl).find(class_="detail-box")
                novel["genre"] = novelDetails.find(class_="meta-path").find('strong').text.strip()

                # 0 = 독점 아님, 1 = 선독점, 2 = 독점
                try:
                    exclusive = novelDetails.select_one('a').find('span').text.strip()

                    if (exclusive == "독점"):
                        novel["exclusive"] = 2

                    elif (exclusive == "선독점"):
                        novel["exclusive"] = 1

                except:
                    novel["exclusive"] = 0

                novel["start_favs"] = extractVal(novelDetails.find(class_="trigger-subscribe").find('b'))
                novel["end_favs"] = -1

                novelDetails = novelDetails.select('dl')[-1].select('dd')

                novel["chapters"] = extractVal(novelDetails[0])
                novel["characters"] = extractVal(novelDetails[3])
                novel["start_views"] = extractVal(novelDetails[1])
                novel["end_views"] = -1
                novel["start_likes"] = extractVal(novelDetails[2])
                novel["end_likes"] = -1
                novel["start_time"] = currentTime
                novel["end_time"] = -1

                newNovels.append(novel)

                # schedule checkLater function for this novel
                laterTime = currentTime + timedelta(minutes=15)
                laterTime = str(laterTime.hour).rjust(2, '0') + ':' + str(laterTime.minute).rjust(2, '0')
                schedule.every().day.at(laterTime).do(checkLater, novel)

            except Exception as e:
                print("ERROR AT " + str(novel["id"]))
                print(e)

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