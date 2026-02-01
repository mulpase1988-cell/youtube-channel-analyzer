# ========================================
# 글로벌 핫데이터 수집기 - Turso DB 버전
# ========================================

import requests
import re
import time
import gspread
import os
import json
import tempfile
from google.oauth2.service_account import Credentials
from datetime import datetime

# ========================================
# 환경변수에서 인증 정보 로드
# ========================================
SERVICE_ACCOUNT_JSON = os.environ.get('GOOGLE_SERVICE_ACCOUNT')
TURSO_URL = os.environ.get('TURSO_URL')
TURSO_TOKEN = os.environ.get('TURSO_TOKEN')

if not SERVICE_ACCOUNT_JSON or not TURSO_URL or not TURSO_TOKEN:
    raise Exception("❌ 필수 환경변수 누락: GOOGLE_SERVICE_ACCOUNT, TURSO_URL, TURSO_TOKEN")

# JSON을 임시 파일로 저장
with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
    f.write(SERVICE_ACCOUNT_JSON)
    SERVICE_ACCOUNT_FILE = f.name

SHEET_NAME = os.environ.get('SHEET_NAME', '유튜브보물창고_테스트')

# ========================================
# Turso 쿼리 함수
# ========================================
def execute_turso_query(sql, args=None):
    """Turso DB에 쿼리 실행"""
    headers = {
        'Authorization': f'Bearer {TURSO_TOKEN}',
        'Content-Type': 'application/json'
    }
    
    turso_api_url = TURSO_URL.replace('libsql://', 'https://') + '/v2/pipeline'
    
    payload = {
        'requests': [
            {
                'type': 'execute',
                'stmt': {
                    'sql': sql,
                    'args': args if args else []
                }
            }
        ]
    }
    
    response = requests.post(turso_api_url, json=payload, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"Turso 쿼리 실행 실패: {response.text}")
    
    return response.json()

def insert_to_turso(data_rows):
    """Turso DB에 데이터 삽입"""
    if not data_rows:
        return 0
    
    # 기존 데이터 삭제
    execute_turso_query("DELETE FROM global_hot_data")
    
    # 새 데이터 삽입
    inserted = 0
    for row in data_rows:
        try:
            sql = """
            INSERT INTO global_hot_data 
            (collect_datetime, country, category, detail_type, ranking, thumbnail, 
             video_title, view_count, channel_name, handle, subscriber_count, tags, 
             video_link, channel_id, thumbnail_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            # ← 여기서부터 추가
            args = [
                row[0],                          # collect_datetime
                row[1],                          # country
                row[2],                          # category
                row[3],                          # detail_type
                int(row[4]) if row[4] else 0,   # ranking (정수)
                row[5],                          # thumbnail
                row[6],                          # video_title
                int(row[7]) if row[7] else 0,   # view_count (정수)
                row[8],                          # channel_name
                row[9],                          # handle
                int(row[10]) if row[10] else 0, # subscriber_count (정수)
                row[11],                         # tags
                row[12],                         # video_link
                row[13],                         # channel_id
                row[14]                          # thumbnail_url
            ]
            # ← 여기까지 추가
            
            execute_turso_query(sql, args)  # ← row를 args로 변경
            inserted += 1
        except Exception as e:
            print(f"⚠️  행 삽입 실패: {str(e)}")
    
    return inserted


# ========================================
# 헬퍼 함수
# ========================================
def parse_duration(duration):
    """YouTube duration을 초 단위로 변환"""
    hours = re.search(r'(\d+)H', duration)
    minutes = re.search(r'(\d+)M', duration)
    seconds = re.search(r'(\d+)S', duration)
    return (int(hours.group(1)) * 3600 if hours else 0) + \
           (int(minutes.group(1)) * 60 if minutes else 0) + \
           (int(seconds.group(1)) if seconds else 0)

# ========================================
# 메인 수집 함수
# ========================================
def run_final_collector():
    """글로벌 핫데이터 수집 및 Turso DB에 저장"""
    print("=" * 60)
    print("🔥 글로벌 핫데이터 수집기 v2.0 (Turso DB)")
    print("=" * 60)
    print(f"🚀 수집 시작 (미드폼 2분 미만 / 태그 공백 처리)\n")
    
    try:
        # Google Sheets 연결
        print("📊 Google Sheets 연결 중...")
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
        client = gspread.authorize(creds)
        spreadsheet = client.open(SHEET_NAME)
        print(f"✅ '{SHEET_NAME}' 연결 완료\n")
        
        # API 키 로드
        print("🔑 API 키 로드 중...")
        api_sheet = spreadsheet.worksheet('API_키_관리')
        api_data = api_sheet.get_all_values()[3:]
        active_keys = [row[2].strip() for row in api_data if len(row) > 2 and row[2].strip().startswith('AIza')]
        print(f"✅ 활성 API 키 {len(active_keys)}개 로드\n")

        if not active_keys:
            print("❌ 활성화된 API 키가 없습니다!")
            return
        
        # 국가 설정 로드
        print("🌍 국가 설정 로드 중...")
        countries = [
            d for d in spreadsheet.worksheet('설정_국가').get_all_records() 
            if str(d.get('수집여부')).upper() == 'Y'
        ]
        print(f"✅ 수집 대상 국가: {len(countries)}개")
        for c in countries:
            print(f"   - {c['국가명']} ({c['국가코드']})")
        
        # 카테고리 설정 로드
        print("\n📂 카테고리 설정 로드 중...")
        categories = [
            d for d in spreadsheet.worksheet('설정_카테고리').get_all_records() 
            if str(d.get('수집여부')).upper() == 'Y'
        ]
        print(f"✅ 수집 대상 카테고리: {len(categories)}개")
        for cat in categories:
            print(f"   - {cat['카테고리명']} (ID: {cat['카테고리ID']})")
        
        print("\n" + "=" * 60)
        print(f"📊 총 {len(countries)} × {len(categories)} = {len(countries) * len(categories)}개 조합 수집 시작")
        print("=" * 60 + "\n")
        
        all_results = []
        key_idx = 0
        success_count = 0
        fail_count = 0

        for country_idx, country in enumerate(countries, 1):
            for cat_idx, cat in enumerate(categories, 1):
                current_key = active_keys[key_idx % len(active_keys)]
                combo_num = (country_idx - 1) * len(categories) + cat_idx
                total_combos = len(countries) * len(categories)
                
                print(f"🔍 [{combo_num}/{total_combos}] {country['국가명']} - {cat['카테고리명']} 수집 중...")
                
                try:
                    # YouTube API 호출
                    v_url = (
                        f"https://www.googleapis.com/youtube/v3/videos"
                        f"?part=snippet,statistics,contentDetails"
                        f"&chart=mostPopular"
                        f"&regionCode={country['국가코드']}"
                        f"&videoCategoryId={cat['카테고리ID']}"
                        f"&maxResults=50"
                        f"&key={current_key}"
                    )
                    v_res = requests.get(v_url, timeout=30).json()

                    if 'items' in v_res:
                        # 채널 정보 수집
                        c_ids = [i['snippet']['channelId'] for i in v_res['items']]
                        c_url = (
                            f"https://www.googleapis.com/youtube/v3/channels"
                            f"?part=snippet,statistics"
                            f"&id={','.join(c_ids)}"
                            f"&key={current_key}"
                        )
                        c_res = requests.get(c_url, timeout=30).json()
                        
                        c_map = {
                            c['id']: {
                                'handle': c['snippet'].get('customUrl', 'N/A'),
                                'subs': c['statistics'].get('subscriberCount', 0)
                            }
                            for c in c_res.get('items', [])
                        }

                        # 영상 데이터 처리
                        for idx, item in enumerate(v_res['items'], 1):
                            snip = item['snippet']
                            stat = item['statistics']
                            cdet = item['contentDetails']
                            c_id = snip['channelId']
                            c_info = c_map.get(c_id, {'handle': 'N/A', 'subs': 0})
                            
                            dur = parse_duration(cdet['duration'])
                            if dur <= 60:
                                d_type = "Shorts"
                            elif dur < 120:
                                d_type = "Mid-form"
                            else:
                                d_type = "Long-form"

                            tags = ", ".join(snip.get('tags', [])[:10]) if snip.get('tags') else ""

                            all_results.append([
                                datetime.now().strftime('%Y-%m-%d %H:%M'),
                                country['국가명'],
                                cat['카테고리명'],
                                d_type,
                                idx,
                                snip["thumbnails"]["medium"]["url"],
                                snip['title'],
                                int(stat.get('viewCount', 0)),
                                snip['channelTitle'],
                                c_info['handle'],
                                int(c_info['subs']),
                                tags,
                                f"https://www.youtube.com/watch?v={item['id']}",
                                c_id,
                                snip['thumbnails']['medium']['url']
                            ])
                        
                        success_count += 1
                        print(f"   ✅ {len(v_res['items'])}개 영상 수집 완료")
                    else:
                        fail_count += 1
                        print(f"   ⚠️  데이터 없음")
                        key_idx += 1
                    
                    time.sleep(0.1)
                    
                except Exception as e:
                    fail_count += 1
                    print(f"   ❌ 에러: {e}")
                    key_idx += 1

        # Turso DB에 저장
        print("\n" + "=" * 60)
        print("💾 Turso DB에 저장 중...")
        print("=" * 60)
        
        if all_results:
            inserted = insert_to_turso(all_results)
            
            print("\n" + "=" * 60)
            print("✅ DB 업데이트 완료!")
            print("=" * 60)
            print(f"📊 총 수집: {len(all_results)}개 영상")
            print(f"💾 DB 저장: {inserted}개")
            print(f"✅ 성공: {success_count}개 조합")
            print(f"❌ 실패: {fail_count}개 조합")
            print("=" * 60)
        else:
            print("⚠️  수집된 데이터가 없습니다.")

    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()

# ========================================
# 실행
# ========================================
if __name__ == '__main__':
    run_final_collector()
