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
        'platformGenres': ["무협", "판타지", "퓨전", "게임", "스포츠", "로맨스", "라이트노벨", "현대판타지", "대체역사", "전쟁·밀리터리", "SF", "추리", "공포·미스테리", "일반소설", "시·수필", "중·단편", "아동소설·동화", "드라마", "연극·시나리오", "BL", "팬픽·패러디"],
        'totalViews': -1,
        'totalNovels': -1,
        'avgViews': -1,
        'avgChapters': -1,
    },

    'heatmapData': {
        'views': [],
        'mostViews': -1,
        'uploads': [],
        'mostUploads': -1,
        'bestTimes': [],
    },

    'genreData': [],

    'monopolyData': [],

    'uploadPeriodData': [],

    'keywordsTagsData': {
        'keywordData': [],
        'tagData': [],
    }
}

# platforms = ['munpia', 'novelpia', 'kakaopage', 'kakaostage', 'navernovel']
platforms = ['munpia']
a = []
for platform in platforms:
    sql = "select * from extendednovelData where novelInstanceId = maxNovelInstanceIId and platform = '" + platform + "' and start_time >= subdate(current_timestamp, 7);"
    cur.execute(sql)
    rows = cur.fetchall()

    total_views = 0
    total_novels = 0
    total_chapters = 0
    total_upload_periods = 0

    for row in rows:
        if (row["end_total_views"] != None):
            total_views += row["end_total_views"]
        total_novels += 1
        total_chapters += row["chapters"]
        total_upload_periods += row["weeklyUploadCount"]

    munpiaData['platformInfoData']['totalViews'] = total_views
    munpiaData['platformInfoData']['totalNovels'] = total_novels
    munpiaData['platformInfoData']['avgViews'] = int(total_views / total_novels)
    munpiaData['platformInfoData']['avgChapters'] = int(total_chapters / total_novels)

    current_time = datetime.today()
    for weekday in range(7):
        print(weekday)
        for row in rows:
            if (row["start_time"].day == (current_time - timedelta(days=current_time.weekday())).day or current_time.weekday() == weekday):
                print(row["start_time"])

    # print(platform, total_novels, total_views, total_views/total_novels, total_chapters/total_novels, total_upload_periods/total_novels)

print(json.dumps(munpiaData, indent=4, sort_keys=True))
# print(datetime.now().weekday() - timedelta(days=current_time.weekday())).day)

date = []
date_time_str = '2022-07-31 01:14:22'
print(datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S').day)

conn.commit()