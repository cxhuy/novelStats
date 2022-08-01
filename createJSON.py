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
        'platformPricings': ["무료 작가연재", "무료 일반연재", "유료 연재작"],
        'platformMonopoly': ["비독점", "선독점", "독점"],
        'platformGenres': ["무협", "판타지", "퓨전", "게임", "스포츠", "로맨스", "라이트노벨", "현대판타지", "대체역사", "전쟁·밀리터리", "SF", "추리", "공포·미스테리", "일반소설", "시·수필", "중·단편", "아동소설·동화", "드라마", "연극·시나리오", "BL", "팬픽·패러디"],
        'totalViews': -1,
        'totalNovels': -1,
        'avgViews': -1,
        'avgChapters': -1,
    },

    'heatmapData': {
        'views': {},
        'mostViews': -1,
        'uploads': {},
        'mostUploads': -1,
        'bestTimes': [],
    },

    'genreData': {},

    'monopolyData': {},

    'uploadPeriodData': {},

    'keywordsTagsData': {
        'keywordData': {},
        'tagData': {},
    }
}

# platforms = ['munpia', 'novelpia', 'kakaopage', 'kakaostage', 'navernovel']
platforms = ["munpia"]

for platform in platforms:
    sql = "select * from extendednovelData where novelInstanceId = maxNovelInstanceIId and platform = '" + platform + "' and start_time >= subdate(current_timestamp, 7);"
    cur.execute(sql)
    rows = cur.fetchall()

    total_views = 0
    total_novels = 0
    total_chapters = 0
    total_upload_periods = 0

    for i in range(7):
        eval(platform + "Data")["heatmapData"]["views"][i] = {}
        eval(platform + "Data")["heatmapData"]["uploads"][i] = {}
        for j in range(24):
            eval(platform + "Data")["heatmapData"]["views"][i][j] = 0
            eval(platform + "Data")["heatmapData"]["uploads"][i][j] = 0

    for genre in eval(platform + "Data")["platformInfoData"]["platformGenres"]:
        eval(platform + "Data")["genreData"][genre] = {}

    for monopoly in eval(platform + "Data")["platformInfoData"]["platformMonopoly"]:
        eval(platform + "Data")["monopolyData"][monopoly] = {}

    for i in range(1, 8):
        eval(platform + "Data")["uploadPeriodData"][i] = {}

    for row in rows:
        if (row["end_total_views"] != None):
            total_views += row["end_total_views"]
        total_novels += 1
        total_chapters += row["chapters"]
        total_upload_periods += row["weeklyUploadCount"]
        eval(platform + "Data")["heatmapData"]["views"][row["start_time"].weekday()][row["start_time"].hour] += \
            row["end_total_views"] - row["start_total_views"] if row["end_total_views"] - row["start_total_views"] > 0 else 0
        eval(platform + "Data")["heatmapData"]["uploads"][row["start_time"].weekday()][row["start_time"].hour] += 1

    eval(platform + "Data")["platformInfoData"]["totalViews"] = total_views
    eval(platform + "Data")["platformInfoData"]["totalNovels"] = total_novels
    eval(platform + "Data")["platformInfoData"]["avgViews"] = int(total_views / total_novels)
    eval(platform + "Data")["platformInfoData"]["avgChapters"] = int(total_chapters / total_novels)

    viewList = []
    uploadList = []
    for i in range(7):
        viewList += list(eval(platform + "Data")["heatmapData"]["views"][i].values())
        uploadList += list(eval(platform + "Data")["heatmapData"]["uploads"][i].values())
    eval(platform + "Data")["heatmapData"]["mostViews"] = max(viewList)
    eval(platform + "Data")["heatmapData"]["mostUploads"] = max(uploadList)

    avgViewList = [int(viewList[x]/uploadList[x]) if uploadList[x] != 0 else 0 for x in range(7*24)]
    for i in range(5):
        index = avgViewList.index(max(avgViewList))
        eval(platform + "Data")["heatmapData"]["bestTimes"].append(["월", "화", "수", "목", "금", "토", "일"][int(index/24)] + " " + str(index%24).rjust(2, '0') + ":00 ~ " + str(index%24 + 1).rjust(2, '0') + ":00")
        avgViewList[index] = 0

print(json.dumps(munpiaData, indent=4, sort_keys=True))

conn.commit()