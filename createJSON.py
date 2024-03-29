import json, os, pymysql
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

conn = pymysql.connect(
    host=os.environ.get('dbhost'),
    user=os.environ.get('dbuser'),
    password=os.environ.get('dbpassword'),
    db=os.environ.get('dbname'),
    charset='utf8'
)

cur = pymysql.cursors.DictCursor(conn)

munpiaData = {
    'platformInfoData': {
        'platformName': "문피아",
        'platformYear': 2012,
        'platformMonth': 12,
        'platformGenres': ["모든 장르", "무협", "판타지", "퓨전", "게임", "스포츠", "로맨스", "라이트노벨", "현대판타지", "대체역사", "전쟁·밀리터리", "SF", "추리",
                           "공포·미스테리", "일반소설", "시·수필", "중·단편", "아동소설·동화", "드라마", "연극·시나리오", "BL", "팬픽·패러디"],
        'platformMonopoly': ["비독점", "선독점", "독점"],
        'platformPricings': ["무료 작가연재", "무료 일반연재", "유료 연재작"],
        'totalViews': -1,
        'totalNovels': -1,
        'avgViews': -1,
        'avgChapters': -1,
    },
}

novelpiaData = {
    'platformInfoData': {
        'platformName': "노벨피아",
        'platformYear': 2021,
        'platformMonth': 1,
        'platformGenres': ["모든 장르", "판타지", "무협", "현대", "로맨스", "현대판타지", "라이트노벨", "공포", "SF", "스포츠", "대체역사", "기타", "패러디"],
        'platformMonopoly': ["비독점", "독점"],
        'platformPricings': ["자유연재", "플러스"],
        'totalViews': -1,
        'totalNovels': -1,
        'avgViews': -1,
        'avgChapters': -1,
    },
}

kakaopageData = {
    'platformInfoData': {
        'platformName': "카카오페이지",
        'platformYear': 2013,
        'platformMonth': 4,
        'platformGenres': ["모든 장르", "판타지", "현판", "로맨스", "로판", "무협"],
        'platformMonopoly': ["비독점", "독점"],
        'platformPricings': [],
        'totalViews': -1,
        'totalNovels': -1,
        'avgViews': -1,
        'avgChapters': -1,
    },
}

kakaostageData = {
    'platformInfoData': {
        'platformName': "카카오페이지 스테이지",
        'platformYear': 2021,
        'platformMonth': 9,
        'platformGenres': ["모든 장르", "판타지", "현판", "무협", "로맨스", "로판", "BL", "자유"],
        'platformMonopoly': ["비독점", "독점"],
        'platformPricings': [],
        'totalViews': -1,
        'totalNovels': -1,
        'avgViews': -1,
        'avgChapters': -1,
    },
}

navernovelData = {
    'platformInfoData': {
        'platformName': "네이버 웹소설",
        'platformYear': 2013,
        'platformMonth': 1,
        'platformGenres': ["모든 장르", "로맨스", "로판", "판타지", "현판", "무협", "미스터리", "라이트노벨"],
        'platformMonopoly': [],
        'platformPricings': [],
        'totalViews': -1,
        'totalNovels': -1,
        'avgViews': -1,
        'avgChapters': -1,
    },
}

platforms = ['munpia', 'novelpia', 'kakaopage', 'kakaostage', 'navernovel']

for platform in platforms:
    total_views = 0
    total_novels = 0
    total_chapters = 0

    eval(platform + "Data")["heatmapData"] = {}
    eval(platform + "Data")["genreData"] = {}
    eval(platform + "Data")["monopolyData"] = {}
    eval(platform + "Data")["pricingData"] = {}
    eval(platform + "Data")["weeklyUploadCountData"] = {}
    eval(platform + "Data")["keywordsTagsData"] = {
        'keywordData': {},
        'tagData': {},
    }

    for platformGenre in eval(platform + "Data")["platformInfoData"]["platformGenres"]:
        eval(platform + "Data")["heatmapData"][platformGenre] = {
            'views': {},
            'mostViews': -1,
            'uploads': {},
            'mostUploads': -1,
            'bestTimes': [],
        }
        for i in range(7):
            eval(platform + "Data")["heatmapData"][platformGenre]["views"][i] = {}
            eval(platform + "Data")["heatmapData"][platformGenre]["uploads"][i] = {}
            for j in range(24):
                eval(platform + "Data")["heatmapData"][platformGenre]["views"][i][j] = 0
                eval(platform + "Data")["heatmapData"][platformGenre]["uploads"][i][j] = 0

    for genre in eval(platform + "Data")["platformInfoData"]["platformGenres"]:
        eval(platform + "Data")["genreData"][genre] = {
            'novelCount': 0,
            'totalViews': 0,
            'avgViews': 0,
            'totalLikes': 0,
            'avgLikes': 0,
            'totalFavs': 0,
            'avgFavs': 0,
        }

    for monopoly in eval(platform + "Data")["platformInfoData"]["platformMonopoly"]:
        eval(platform + "Data")["monopolyData"][monopoly] = {
            'novelCount': 0,
            'totalViews': 0,
            'avgViews': 0,
            'totalLikes': 0,
            'avgLikes': 0,
            'totalFavs': 0,
            'avgFavs': 0,
        }

    for pricing in eval(platform + "Data")["platformInfoData"]["platformPricings"]:
        eval(platform + "Data")["pricingData"][pricing] = {
            'novelCount': 0,
            'totalViews': 0,
            'avgViews': 0,
            'totalLikes': 0,
            'avgLikes': 0,
            'totalFavs': 0,
            'avgFavs': 0,
        }

    for i in range(1, 8):
        eval(platform + "Data")["weeklyUploadCountData"][i] = {
            'novelCount': 0,
            'totalViews': 0,
            'avgViews': 0,
            'totalLikes': 0,
            'avgLikes': 0,
            'totalFavs': 0,
            'avgFavs': 0,
        }

    sql = "select * from extendedNovelData where platform = %s and " \
          "start_time >= subdate(subdate(current_timestamp, interval 1 hour), 7) order by novelInstanceId;"
    cur.execute(sql, (platform))
    rows = cur.fetchall()
    conn.commit()

    sql = "select novelInstanceId, genre from genres where novelInstanceId >= %s"
    cur.execute(sql, (rows[0]["novelInstanceId"]))
    genres = cur.fetchall()
    conn.commit()

    rowGenres = {}
    for genre in genres:
        if (genre["novelInstanceId"] not in rowGenres):
            rowGenres[genre["novelInstanceId"]] = []
        rowGenres[genre["novelInstanceId"]].append(genre["genre"])

    for row in rows:
        if (row["end_total_views"] == None or row["end_total_views"] > -1):
            if platform in ['munpia', 'novelpia', 'kakaopage', 'kakaostage']:
                eval(platform + "Data")["heatmapData"]["모든 장르"]["views"][row["start_time"].weekday()][
                    row["start_time"].hour] += \
                    row["end_total_views"] - row["start_total_views"] if row["end_total_views"] - row[
                        "start_total_views"] > 0 else 0
            else:
                eval(platform + "Data")["heatmapData"]["모든 장르"]["views"][row["start_time"].weekday()][
                    row["start_time"].hour] += \
                    row["end_recent_views"] - row["start_recent_views"] if row["end_recent_views"] - row[
                        "start_recent_views"] > 0 else 0

            eval(platform + "Data")["heatmapData"]["모든 장르"]["uploads"][row["start_time"].weekday()][
                row["start_time"].hour] += 1

            if (row["novelInstanceId"] in rowGenres):
                for rowGenre in rowGenres[row["novelInstanceId"]]:
                    if platform in ['munpia', 'novelpia', 'kakaopage', 'kakaostage']:
                        eval(platform + "Data")["heatmapData"][rowGenre]["views"][row["start_time"].weekday()][row["start_time"].hour] += \
                            row["end_total_views"] - row["start_total_views"] if row["end_total_views"] - row[
                                "start_total_views"] > 0 else 0
                    else:
                        eval(platform + "Data")["heatmapData"][rowGenre]["views"][row["start_time"].weekday()][
                            row["start_time"].hour] += \
                            row["end_recent_views"] - row["start_recent_views"] if row["end_recent_views"] - row[
                                "start_recent_views"] > 0 else 0

                    eval(platform + "Data")["heatmapData"][rowGenre]["uploads"][row["start_time"].weekday()][
                        row["start_time"].hour] += 1

    sql = "select * from extendedNovelData where novelInstanceId = maxNovelInstanceId and platform = %s " \
          "and start_time >= subdate(subdate(current_timestamp, interval 1 hour), 7);"
    cur.execute(sql, (platform))
    rows = cur.fetchall()
    conn.commit()

    if platform in ["munpia", "novelpia"]:
        dataTypes = ["monopoly", "pricing", "weeklyUploadCount"]
    elif platform in ["kakaopage", "kakaostage"]:
        dataTypes = ["monopoly", "weeklyUploadCount"]
    else:
        dataTypes = ["weeklyUploadCount"]

    for row in rows:
        if platform in ['munpia', 'novelpia', 'kakaopage', 'kakaostage']:
            total_views += max(row["start_total_views"], row["end_total_views"])
        total_novels += 1
        total_chapters += row["chapters"]

        if (row["novelInstanceId"] in rowGenres):
            for rowGenre in rowGenres[row["novelInstanceId"]]:
                eval(platform + "Data")["genreData"][rowGenre]["novelCount"] += 1

                if (platform in ["munpia", "novelpia", "kakaostage", "kakaopage"]):
                    eval(platform + "Data")["genreData"][rowGenre]["totalViews"] += max(row["start_total_views"], row["end_total_views"])
                    eval(platform + "Data")["genreData"][rowGenre]["avgViews"] = int(
                        eval(platform + "Data")["genreData"][rowGenre]["totalViews"] /
                        eval(platform + "Data")["genreData"][rowGenre]["novelCount"])

                eval(platform + "Data")["genreData"][rowGenre]["totalLikes"] += max(row["start_total_likes"], row["end_total_likes"])
                eval(platform + "Data")["genreData"][rowGenre]["avgLikes"] = int(
                    eval(platform + "Data")["genreData"][rowGenre]["totalLikes"] /
                    eval(platform + "Data")["genreData"][rowGenre]["novelCount"])

                if (platform in ["munpia", "novelpia", "kakaostage"]):
                    eval(platform + "Data")["genreData"][rowGenre]["totalFavs"] += max(row["start_favs"], row["end_favs"])
                    eval(platform + "Data")["genreData"][rowGenre]["avgFavs"] = int(
                        eval(platform + "Data")["genreData"][rowGenre]["totalFavs"] /
                        eval(platform + "Data")["genreData"][rowGenre]["novelCount"])

        for dataType in dataTypes:
            if (dataType == "weeklyUploadCount" and row[dataType] > 7):
                row[dataType] = 7

            eval(platform + "Data")[dataType + "Data"][row[dataType]]["novelCount"] += 1

            if (platform in ["munpia", "novelpia", "kakaostage", "kakaopage"]):
                eval(platform + "Data")[dataType + "Data"][row[dataType]]["totalViews"] += max(row["start_total_views"], row["end_total_views"])
                eval(platform + "Data")[dataType + "Data"][row[dataType]]["avgViews"] = int(
                    eval(platform + "Data")[dataType + "Data"][row[dataType]]["totalViews"] /
                    eval(platform + "Data")[dataType + "Data"][row[dataType]]["novelCount"])

            eval(platform + "Data")[dataType + "Data"][row[dataType]]["totalLikes"] += max(row["start_total_likes"], row["end_total_likes"])
            eval(platform + "Data")[dataType + "Data"][row[dataType]]["avgLikes"] = int(
                eval(platform + "Data")[dataType + "Data"][row[dataType]]["totalLikes"] /
                eval(platform + "Data")[dataType + "Data"][row[dataType]]["novelCount"])

            if (platform in ["munpia", "novelpia", "kakaostage"]):
                eval(platform + "Data")[dataType + "Data"][row[dataType]]["totalFavs"] += max(row["start_favs"], row["end_favs"])
                eval(platform + "Data")[dataType + "Data"][row[dataType]]["avgFavs"] = int(
                    eval(platform + "Data")[dataType + "Data"][row[dataType]]["totalFavs"] /
                    eval(platform + "Data")[dataType + "Data"][row[dataType]]["novelCount"])

    eval(platform + "Data")["platformInfoData"]["totalViews"] = total_views
    eval(platform + "Data")["platformInfoData"]["totalNovels"] = total_novels
    eval(platform + "Data")["platformInfoData"]["avgViews"] = int(total_views / total_novels)
    eval(platform + "Data")["platformInfoData"]["avgChapters"] = int(total_chapters / total_novels)

    for platformGenre in eval(platform + "Data")["platformInfoData"]["platformGenres"]:
        viewList = []
        uploadList = []
        for i in range(7):
            viewList += list(eval(platform + "Data")["heatmapData"][platformGenre]["views"][i].values())
            uploadList += list(eval(platform + "Data")["heatmapData"][platformGenre]["uploads"][i].values())
        eval(platform + "Data")["heatmapData"][platformGenre]["mostViews"] = max(viewList)
        eval(platform + "Data")["heatmapData"][platformGenre]["mostUploads"] = max(uploadList)

        avgViewList = [int(viewList[x]/uploadList[x]) if uploadList[x] != 0 else 0 for x in range(7*24)]
        for i in range(5):
            if (max(avgViewList) == 0):
                break
            index = avgViewList.index(max(avgViewList))
            eval(platform + "Data")["heatmapData"][platformGenre]["bestTimes"].append(["월", "화", "수", "목", "금", "토", "일"][int(index/24)] + " " + str(index%24).rjust(2, '0') + ":00 ~ " + str(index%24 + 1).rjust(2, '0') + ":00")
            avgViewList[index] = 0

    if (platform in ["munpia", "novelpia", "kakaopage", "kakaostage"]):
        sql = "select *, totalViewCount / keywordCount as avgViewCount from (select keyword, count(*) as keywordCount, sum(viewCount) " \
              "as totalViewCount from longKeywordsWithViewCount where novelInstanceId in (select max(novelInstanceId) " \
              "as maxNovelInstanceId from novelData where platform = %s " \
              "and start_time >= subdate(subdate(current_timestamp, interval 1 hour), 7) " \
              "group by novelId) group by keyword order by keywordCount desc limit 20) tempTable;"
        cur.execute(sql, (platform))
        topKeywords = cur.fetchall()
        conn.commit()

        keywordRank = 1
        for keyword in topKeywords:
            eval(platform + "Data")["keywordsTagsData"]["keywordData"][keywordRank] = {
                'keywordName': keyword["keyword"],
                'keywordCount': keyword["keywordCount"],
                'keywordAvgViewCount': int(keyword["avgViewCount"])
            }
            keywordRank += 1

    if (platform in ["munpia", "novelpia"]):
        sql = "select *, totalViewCount / tagCount as avgViewCount from (select tag, count(*) as tagCount, sum(viewCount) " \
              "as totalViewCount from tagsWithViewCount where novelInstanceId in (select max(novelInstanceId) " \
              "as maxNovelInstanceId from novelData where platform = %s " \
              "and start_time >= subdate(subdate(current_timestamp, interval 1 hour), 7) " \
              "group by novelId) group by tag order by tagCount desc limit 20) tempTable;"
        cur.execute(sql, (platform))
        topTags = cur.fetchall()
        conn.commit()

        tagRank = 1
        for tag in topTags:
            eval(platform + "Data")["keywordsTagsData"]["tagData"][tagRank] = {
                'tagName': tag["tag"],
                'tagCount': tag["tagCount"],
                'tagAvgViewCount': int(tag["avgViewCount"])
            }
            tagRank += 1

    eval(platform + "Data")["genreData"] = dict(sorted(eval(platform + "Data")["genreData"].items(),
                                                       key=lambda item: item[1]["novelCount"], reverse=True))
    eval(platform + "Data")["monopolyData"] = dict(sorted(eval(platform + "Data")["monopolyData"].items(),
                                                       key=lambda item: item[1]["novelCount"], reverse=True))
    eval(platform + "Data")["pricingData"] = dict(sorted(eval(platform + "Data")["pricingData"].items(),
                                                       key=lambda item: item[1]["novelCount"], reverse=True))

    # print(json.dumps(eval(platform + "Data"), indent=4))
    print(platform + ".json is finished")
    with open(platform + '.json', 'w', encoding='utf-8') as f:
        json.dump(eval(platform + "Data"), f, ensure_ascii=False, indent=4)

