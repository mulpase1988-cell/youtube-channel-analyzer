# ========================================
# 글로벌 핫데이터 수집기 - 별도 설정 테이블 버전
# ========================================

import requests
import re
import time
import gspread
import os
import json
import tempfile
import libsql_client
from google.oauth2.service_account import Credentials
from datetime import datetime

# ========================================
# 환경변수에서 Google 인증 정보 로드
# ========================================
SERVICE_ACCOUNT_JSON = os.environ.get('GOOGLE_SERVICE_ACCOUNT')
if not SERVICE_ACCOUNT_JSON:
    raise Exception("❌ GOOGLE_SERVICE_ACCOUNT 환경변수가 설정되지 않았습니다")

# JSON을 임시 파일로 저장
with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
    f.write(SERVICE_ACCOUNT_JSON)
    SERVICE_ACCOUNT_FILE = f.name

# ========================================
# 초기 Turso 연결 (설정 테이블 로드용)
# ========================================
BOOTSTRAP_TURSO_URL = os.environ.get('TURSO_URL')
BOOTSTRAP_TURSO_TOKEN = os.environ.get('TURSO_TOKEN')

if not BOOTSTRAP_TURSO_URL or not BOOTSTRAP_TURSO_TOKEN:
    raise Exception("❌ 초기 TURSO_URL 또는 TURSO_TOKEN 환경변수가 설정되지 않았습니다")

# ========================================
# 설정 로드 함수
# ========================================
def load_settings_from_db(turso_url, turso_token):
    """Turso의 turso_settings 테이블에서 설정 로드"""
    try:
        print("⚙️  Turso 설정 테이블에서 설정 로드 중...\n")
        
        client = libsql_client.create_client(url=turso_url, auth_token=turso_token)
        
        # 모든 설정 가져오기
        result = client.execute("SELECT setting_key, setting_value FROM turso_settings;")
        
        settings = {}
        for row in result.rows:
            settings[row[0]] = row[1]
        
        if not settings:
            print("⚠️  설정 테이블이 비어있습니다!\n")
            return None
        
        print("✅ 설정 로드 완료:")
        print(f"   - turso_url: {settings.get('turso_url', 'N/A')[:50]}...")
        print(f"   - turso_token: {settings.get('turso_token', 'N/A')[:30]}...")
        print(f"   - sheet_name: {settings.get('sheet_name', 'N/A')}")
        print(f"   - collection_interval: {settings.get('collection_interval_hours', 'N/A')}시간\n")
        
        return settings
        
    except Exception as e:
        print(f"❌ 설정 로드 실패: {e}\n")
        return None

def get_turso_client(url, token):
    """Turso 클라이언트 생성"""
    return libsql_client.create_client(url=url, auth_token=token)

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

def insert_hot_data(db_client, data_row):
    """글로벌 핫데이터 삽입"""
    try:
        sql = """
        INSERT INTO global_hot_data 
        (collect_datetime, country, category, detail_type, ranking, thumbnail, 
         video_title, view_count, channel_name, handle, subscriber_count, tags, 
         video_link, channel_id, thumbnail_url) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        db_client.execute(sql, data_row)
        return True
    except Exception as e:
        print(f"   ⚠️  DB 저장 실패: {e}")
        return False

def update_setting(db_client, setting_key, setting_value):
    """설정값 업데이트"""
    try:
        sql = """
        UPDATE turso_settings 
        SET setting_value = ?, updated_at = CURRENT_TIMESTAMP 
        WHERE setting_key = ?
        """
        db_client.execute(sql, (setting_value, setting_key))
        return True
    except:
        return False

# ========================================
# 메인 수집 함수
# ========================================
def run_final_collector():
    """글로벌 핫데이터 수집 및 Turso 저장"""
    print("=" * 60)
    print("🔥 글로벌 핫데이터 수집기 v4.0")
    print("   (별도 설정 테이블 버전)")
    print("=" * 60 + "\n")
    
    try:
        # 초기 Turso 클라이언트로 설정 로드
        print("🔌 초기 Turso 연결 중...\n")
        bootstrap_client = get_turso_client(BOOTSTRAP_TURSO_URL, BOOTSTRAP_TURSO_TOKEN)
        
        # 설정 로드
        settings = load_settings_from_db(BOOTSTRAP_TURSO_URL, BOOTSTRAP_TURSO_TOKEN)
        if not settings:
            print("❌ 설정을 로드할 수 없습니다")
            return
        
        # 설정값 추출
        turso_url = settings.get('turso_url')
        turso_token = settings.get('turso_token')
        sheet_name = settings.get('sheet_name', '유튜브보물창고_테스트')
        
        if not turso_url or not turso_token:
            print("❌ turso_url 또는 turso_token 설정이 없습니다")
            return
        
        # 메인 Turso 클라이언트 생성 (설정의 URL, Token 사용)
        print(f"🗄️  메인 Turso 연결 중 ({turso_url[:50]}...)\n")
        db_client = get_turso_client(turso_url, turso_token)
        print("✅ Turso 연결 완료\n")
        
        # Google Sheets 연결
        print("📊 Google Sheets 연결 중...")
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
        client = gspread.authorize(creds)
        spreadsheet = client.open(sheet_name)
        print(f"✅ '{sheet_name}' 연결 완료\n")
        
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
        
        # 기존 데이터 삭제
        print("🗑️  기존 핫데이터 삭제 중...")
        try:
            db_client.execute("DELETE FROM global_hot_data;")
            print("✅ 기존 데이터 삭제 완료\n")
        except Exception as e:
            print(f"⚠️  삭제 중 오류: {e}\n")
        
        key_idx = 0
        success_count = 0
        fail_count = 0
        total_inserted = 0
        collection_start_time = datetime.now()

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

                        # 영상 데이터 처리 및 Turso에 저장
                        inserted_count = 0
                        for idx, item in enumerate(v_res['items'], 1):
                            snip = item['snippet']
                            stat = item['statistics']
                            cdet = item['contentDetails']
                            c_id = snip['channelId']
                            c_info = c_map.get(c_id, {'handle': 'N/A', 'subs': 0})
                            
                            # 시간 기준 타입 분류
                            dur = parse_duration(cdet['duration'])
                            if dur <= 60:
                                d_type = "Shorts"
                            elif dur < 120:
                                d_type = "Mid-form"
                            else:
                                d_type = "Long-form"

                            # 태그 처리
                            tags = ", ".join(snip.get('tags', [])[:10]) if snip.get('tags') else ""

                            # Turso에 삽입
                            data_row = (
                                datetime.now().strftime('%Y-%m-%d %H:%M'),
                                country['국가명'],
                                cat['카테고리명'],
                                d_type,
                                idx,
                                snip['thumbnails']['medium']['url'],
                                snip['title'],
                                int(stat.get('viewCount', 0)),
                                snip['channelTitle'],
                                c_info['handle'],
                                int(c_info['subs']),
                                tags,
                                f"https://www.youtube.com/watch?v={item['id']}",
                                c_id,
                                snip['thumbnails']['medium']['url']
                            )
                            
                            if insert_hot_data(db_client, data_row):
                                inserted_count += 1
                                total_inserted += 1
                        
                        success_count += 1
                        print(f"   ✅ {inserted_count}개 영상 저장")
                    else:
                        fail_count += 1
                        print(f"   ⚠️  데이터 없음")
                        key_idx += 1
                    
                    time.sleep(0.1)
                    
                except Exception as e:
                    fail_count += 1
                    print(f"   ❌ 에러: {e}")
                    key_idx += 1

        # 결과 출력 및 설정 업데이트
        print("\n" + "=" * 60)
        print("✅ 수집 완료!")
        print("=" * 60)
        print(f"📊 총 수집: {total_inserted}개 영상")
        print(f"✅ 성공: {success_count}개 조합")
        print(f"❌ 실패: {fail_count}개 조합")
        print(f"⏱️  소요 시간: {(datetime.now() - collection_start_time).total_seconds():.1f}초")
        print("=" * 60)
        
        # 마지막 수집 시간 업데이트
        last_collection_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        update_setting(db_client, 'last_collection_time', last_collection_time)
        print(f"\n⏰ 마지막 수집 시간: {last_collection_time}")

    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()

# ========================================
# 실행
# ========================================
if __name__ == '__main__':
    run_final_collector()
