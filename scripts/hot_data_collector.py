import os
import json
import requests
from datetime import datetime
from google.auth import _helpers
from google.auth.transport.requests import Request
from google.oauth2 import service_account
import gspread

# Turso 설정
TURSO_URL = os.environ.get('TURSO_URL')
TURSO_TOKEN = os.environ.get('TURSO_TOKEN')

def get_turso_api_url():
    """Turso HTTP API URL 생성"""
    return TURSO_URL.replace('libsql://', 'https://') + '/v2/pipeline'

def execute_turso_query(sql, args=None):
    """Turso에서 쿼리 실행"""
    headers = {
        'Authorization': f'Bearer {TURSO_TOKEN}',
        'Content-Type': 'application/json'
    }
    
    statement = {'sql': sql}
    if args:
        statement['args'] = args
    
    payload = {
        'requests': [{'type': 'execute', 'statement': statement}]
    }
    
    response = requests.post(
        get_turso_api_url(),
        json=payload,
        headers=headers
    )
    
    if response.status_code != 200:
        raise Exception(f"Turso 쿼리 실행 실패: {response.text}")
    
    return response.json()

def load_google_service_account():
    """DB에서 Google 서비스 계정 로드"""
    print("🔄 DB에서 Google 서비스 계정 로드 중...")
    
    sql = "SELECT secret_value FROM secrets_management WHERE secret_key = 'google_service_account' AND is_active = 'Y'"
    
    try:
        result = execute_turso_query(sql)
        
        # 응답 파싱
        if result and 'results' in result and len(result['results']) > 0:
            rows = result['results'][0].get('rows', [])
            if rows and len(rows) > 0:
                secret_value = rows[0][0]
                service_account_data = json.loads(secret_value)
                print("✅ Google 서비스 계정 로드 완료")
                return service_account_data
        
        print("❌ secrets_management에서 google_service_account를 찾을 수 없습니다")
        return None
    
    except Exception as e:
        print(f"❌ Google 서비스 계정 로드 실패: {str(e)}")
        return None

def load_turso_settings():
    """DB에서 Turso 설정 로드"""
    print("🔄 DB에서 설정 로드 중...")
    
    sql = "SELECT setting_key, setting_value FROM turso_settings WHERE is_active = 'Y'"
    
    try:
        result = execute_turso_query(sql)
        
        settings = {}
        if result and 'results' in result and len(result['results']) > 0:
            rows = result['results'][0].get('rows', [])
            for row in rows:
                settings[row[0]] = row[1]
        
        print("✅ 설정 로드 완료")
        return settings
    
    except Exception as e:
        print(f"⚠️ 설정 로드 실패 (기본값 사용): {str(e)}")
        return {}

def load_active_api_keys(gc):
    """Google Sheets에서 활성 API 키 로드"""
    print("🔄 API 키 로드 중...")
    
    try:
        sheet_name = '유튜브보물창고_테스트'
        sheet = gc.open(sheet_name)
        
        worksheet = sheet.worksheet('API_키_관리')
        all_values = worksheet.get_all_values()
        
        api_keys = []
        for row in all_values[1:]:  # 헤더 제외
            if len(row) >= 5:  # 충분한 컬럼 확인
                api_key = row[2]  # 3번째 컬럼: API 키
                status = row[3]   # 4번째 컬럼: 상태
                is_active = row[14] if len(row) > 14 else 'FALSE'  # 마지막 컬럼: 활성화
                
                if api_key.startswith('AIza') and is_active.upper() == 'TRUE':
                    api_keys.append(api_key)
        
        if api_keys:
            print(f"✅ {len(api_keys)}개의 활성 API 키 로드 완료")
            return api_keys
        else:
            print("❌ 활성 API 키가 없습니다")
            return []
    
    except Exception as e:
        print(f"❌ API 키 로드 실패: {str(e)}")
        return []

def load_countries_to_collect(gc):
    """Google Sheets에서 수집 대상 국가 로드"""
    print("🔄 수집 대상 국가 로드 중...")
    
    try:
        sheet_name = '유튜브보물창고_테스트'
        sheet = gc.open(sheet_name)
        
        worksheet = sheet.worksheet('설정_국가')
        all_values = worksheet.get_all_values()
        
        countries = []
        for row in all_values[1:]:  # 헤더 제외
            if len(row) >= 3 and row[2].upper() == 'Y':  # 수집여부가 Y
                countries.append({
                    'name': row[0],   # 국가명
                    'code': row[1]    # 국가코드
                })
        
        print(f"✅ {len(countries)}개 국가 로드 완료")
        return countries
    
    except Exception as e:
        print(f"❌ 국가 로드 실패: {str(e)}")
        return []

def load_categories_to_collect(gc):
    """Google Sheets에서 수집 대상 카테고리 로드"""
    print("🔄 수집 대상 카테고리 로드 중...")
    
    try:
        sheet_name = '유튜브보물창고_테스트'
        sheet = gc.open(sheet_name)
        
        worksheet = sheet.worksheet('설정_카테고리')
        all_values = worksheet.get_all_values()
        
        categories = []
        for row in all_values[1:]:  # 헤더 제외
            if len(row) >= 3 and row[2].upper() == 'Y':  # 수집여부가 Y
                categories.append({
                    'name': row[0],   # 카테고리명
                    'id': row[1]      # 카테고리ID
                })
        
        print(f"✅ {len(categories)}개 카테고리 로드 완료")
        return categories
    
    except Exception as e:
        print(f"❌ 카테고리 로드 실패: {str(e)}")
        return []

def clear_hot_data_table():
    """기존 global_hot_data 삭제"""
    print("🔄 기존 데이터 삭제 중...")
    
    try:
        sql = "DELETE FROM global_hot_data"
        execute_turso_query(sql)
        print("✅ 기존 데이터 삭제 완료")
    
    except Exception as e:
        print(f"⚠️ 데이터 삭제 실패: {str(e)}")

def insert_hot_data(data_rows):
    """hot_data를 global_hot_data 테이블에 삽입"""
    print(f"🔄 {len(data_rows)}개 행을 DB에 삽입 중...")
    
    inserted_count = 0
    
    for row in data_rows:
        try:
            sql = """
            INSERT INTO global_hot_data 
            (collect_datetime, country, category, detail_type, ranking, thumbnail, 
             video_title, view_count, channel_name, handle, subscriber_count, tags, 
             video_link, channel_id, thumbnail_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            args = [
                row[0],   # collect_datetime
                row[1],   # country
                row[2],   # category
                row[3],   # detail_type
                row[4],   # ranking
                row[5],   # thumbnail
                row[6],   # video_title
                row[7],   # view_count
                row[8],   # channel_name
                row[9],   # handle
                row[10],  # subscriber_count
                row[11],  # tags
                row[12],  # video_link
                row[13],  # channel_id
                row[14]   # thumbnail_url
            ]
            
            execute_turso_query(sql, args)
            inserted_count += 1
        
        except Exception as e:
            print(f"⚠️ 행 삽입 실패: {str(e)}")
    
    print(f"✅ {inserted_count}/{len(data_rows)}개 행 삽입 완료")
    return inserted_count

def update_last_collection_time():
    """마지막 수집 시간 업데이트"""
    try:
        current_time = datetime.now().isoformat()
        sql = """
        UPDATE turso_settings 
        SET setting_value = ?, last_updated = CURRENT_TIMESTAMP
        WHERE setting_key = 'last_collection_time'
        """
        
        execute_turso_query(sql, [current_time])
        print(f"✅ 마지막 수집 시간 업데이트: {current_time}")
    
    except Exception as e:
        print(f"⚠️ 마지막 수집 시간 업데이트 실패: {str(e)}")

def main():
    """메인 함수"""
    print("="*60)
    print("🎬 글로벌 핫데이터 수집기 시작")
    print("="*60)
    
    # Step 1: Turso 연결 확인
    if not TURSO_URL or not TURSO_TOKEN:
        print("❌ 환경변수 TURSO_URL 또는 TURSO_TOKEN이 설정되지 않았습니다")
        return
    
    print(f"✅ Turso URL: {TURSO_URL[:50]}...")
    
    # Step 2: DB에서 Google 서비스 계정 로드
    service_account_data = load_google_service_account()
    if not service_account_data:
        print("❌ Google 서비스 계정 로드 실패. 프로그램 종료")
        return
    
    # Step 3: 서비스 계정으로 인증
    try:
        credentials = service_account.Credentials.from_service_account_info(
            service_account_data,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        gc = gspread.authorize(credentials)
        print("✅ Google Sheets 인증 성공")
    
    except Exception as e:
        print(f"❌ Google Sheets 인증 실패: {str(e)}")
        return
    
    # Step 4: 설정 로드
    turso_settings = load_turso_settings()
    
    # Step 5: API 키, 국가, 카테고리 로드
    api_keys = load_active_api_keys(gc)
    countries = load_countries_to_collect(gc)
    categories = load_categories_to_collect(gc)
    
    if not api_keys or not countries or not categories:
        print("❌ 필수 설정이 부족합니다. 프로그램 종료")
        return
    
    # Step 6: 기존 데이터 삭제
    clear_hot_data_table()
    
    # Step 7: 데이터 수집 (여기에 YouTube API 호출 로직 추가)
    print("\n🔄 YouTube API에서 데이터 수집 중...")
    print(f"   - API 키: {len(api_keys)}개")
    print(f"   - 국가: {len(countries)}개")
    print(f"   - 카테고리: {len(categories)}개")
    print(f"   - 조합 수: {len(api_keys) * len(countries) * len(categories)}")
    
    # TODO: YouTube API 호출 로직 구현
    # data_rows = collect_from_youtube_api(api_keys, countries, categories)
    # insert_hot_data(data_rows)
    
    # Step 8: 마지막 수집 시간 업데이트
    update_last_collection_time()
    
    print("\n" + "="*60)
    print("✅ 글로벌 핫데이터 수집 완료!")
    print("="*60)

if __name__ == '__main__':
    main()
