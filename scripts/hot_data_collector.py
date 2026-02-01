import os
import json
import requests
import re
from datetime import datetime
from google.oauth2 import service_account
import gspread

def execute_turso_query(turso_url, turso_token, sql, args=None):
    """Turso에서 쿼리 실행"""
    headers = {
        'Authorization': f'Bearer {turso_token}',
        'Content-Type': 'application/json'
    }
    
    turso_api_url = turso_url.replace('libsql://', 'https://') + '/v2/pipeline'
    
    statement = {'sql': sql}
    if args:
        statement['args'] = args
    
    payload = {
        'requests': [{'type': 'execute', 'statement': statement}]
    }
    
    response = requests.post(
        turso_api_url,
        json=payload,
        headers=headers
    )
    
    if response.status_code != 200:
        raise Exception(f"Turso 쿼리 실행 실패: {response.text}")
    
    return response.json()

def bootstrap_turso_credentials():
    """Step 1: 환경변수에서 부트스트랩 Turso 정보 로드"""
    print("🔐 Step 1: 부트스트랩 Turso 정보 로드")
    
    bootstrap_url = os.environ.get('TURSO_URL')
    bootstrap_token = os.environ.get('TURSO_TOKEN')
    
    if not bootstrap_url or not bootstrap_token:
        print("❌ 환경변수 TURSO_URL 또는 TURSO_TOKEN이 설정되지 않았습니다")
        return None, None
    
    print(f"✅ 부트스트랩 정보 로드 완료")
    return bootstrap_url, bootstrap_token

def load_all_credentials_from_db(turso_url, turso_token):
    """Step 2: DB에서 모든 인증 정보 로드"""
    print("\n🔐 Step 2: DB에서 모든 인증 정보 로드")
    
    try:
        # 1) turso_settings 테이블
        sql = "SELECT setting_key, setting_value FROM turso_settings WHERE is_active = 'Y'"
        result = execute_turso_query(turso_url, turso_token, sql)
        
        turso_settings = {}
        if result and 'results' in result and len(result['results']) > 0:
            rows = result['results'][0].get('rows', [])
            for row in rows:
                turso_settings[row[0]] = row[1]
        
        print(f"✅ Turso 설정 로드 완료 ({len(turso_settings)}개 항목)")
        
        # 2) secrets_management 테이블
        sql2 = "SELECT secret_value FROM secrets_management WHERE secret_key = 'google_service_account' AND is_active = 'Y'"
        result2 = execute_turso_query(turso_url, turso_token, sql2)
        
        google_service_account = None
        if result2 and 'results' in result2 and len(result2['results']) > 0:
            rows2 = result2['results'][0].get('rows', [])
            if rows2:
                google_service_account = json.loads(rows2[0][0])
        
        if google_service_account:
            print(f"✅ Google 서비스 계정 로드 완료")
        else:
            print(f"❌ Google 서비스 계정을 찾을 수 없습니다")
            return None, None, None
        
        # 3) api_key_management 테이블
        sql3 = "SELECT api_key, key_name, status FROM api_key_management WHERE is_active = 'TRUE' ORDER BY number ASC"
        result3 = execute_turso_query(turso_url, turso_token, sql3)
        
        api_keys = []
        if result3 and 'results' in result3 and len(result3['results']) > 0:
            rows3 = result3['results'][0].get('rows', [])
            for row in rows3:
                api_key = row[0]
                key_name = row[1]
                status = row[2]
                
                if api_key and api_key.startswith('AIza'):
                    api_keys.append({
                        'key': api_key,
                        'name': key_name,
                        'status': status
                    })
        
        if api_keys:
            print(f"✅ Google API 키 로드 완료 ({len(api_keys)}개)")
        else:
            print(f"❌ 활성 API 키가 없습니다")
            return None, None, None
        
        return turso_settings, google_service_account, api_keys
    
    except Exception as e:
        print(f"❌ DB에서 정보 로드 실패: {str(e)}")
        return None, None, None

def get_final_turso_credentials(turso_settings, bootstrap_url, bootstrap_token):
    """Step 3: 최종 Turso 정보 결정"""
    print("\n🔐 Step 3: 최종 Turso 정보 결정")
    
    final_url = turso_settings.get('turso_url') or bootstrap_url
    final_token = turso_settings.get('turso_token') or bootstrap_token
    
    if turso_settings.get('turso_url'):
        print(f"✅ DB의 turso_url 사용")
    else:
        print(f"✅ 부트스트랩 turso_url 사용")
    
    if turso_settings.get('turso_token'):
        print(f"✅ DB의 turso_token 사용")
    else:
        print(f"✅ 부트스트랩 turso_token 사용")
    
    return final_url, final_token

def load_countries_from_db(turso_url, turso_token):
    """DB에서 수집 대상 국가 로드"""
    print("🌍 수집 대상 국가 로드 중...")
    
    try:
        sql = "SELECT country_name, country_code FROM country_settings WHERE is_active = 'Y' ORDER BY id ASC"
        result = execute_turso_query(turso_url, turso_token, sql)
        
        countries = []
        if result and 'results' in result and len(result['results']) > 0:
            rows = result['results'][0].get('rows', [])
            for row in rows:
                countries.append({
                    'name': row[0],
                    'code': row[1]
                })
        
        if countries:
            print(f"✅ {len(countries)}개 국가 로드 완료")
            return countries
        else:
            print("❌ 수집 대상 국가가 없습니다")
            return []
    
    except Exception as e:
        print(f"❌ 국가 로드 실패: {str(e)}")
        return []

def load_categories_from_db(turso_url, turso_token):
    """DB에서 수집 대상 카테고리 로드"""
    print("📂 수집 대상 카테고리 로드 중...")
    
    try:
        sql = "SELECT category_name, category_id FROM category_config WHERE is_active = 'Y' ORDER BY id ASC"
        result = execute_turso_query(turso_url, turso_token, sql)
        
        categories = []
        if result and 'results' in result and len(result['results']) > 0:
            rows = result['results'][0].get('rows', [])
            for row in rows:
                categories.append({
                    'name': row[0],
                    'id': row[1]
                })
        
        if categories:
            print(f"✅ {len(categories)}개 카테고리 로드 완료")
            return categories
        else:
            print("❌ 수집 대상 카테고리가 없습니다")
            return []
    
    except Exception as e:
        print(f"❌ 카테고리 로드 실패: {str(e)}")
        return []

def clear_hot_data_table(turso_url, turso_token):
    """기존 global_hot_data 삭제"""
    print("\n🗑️  기존 데이터 삭제 중...")
    
    try:
        sql = "DELETE FROM global_hot_data"
        execute_turso_query(turso_url, turso_token, sql)
        print("✅ 기존 데이터 삭제 완료")
    
    except Exception as e:
        print(f"⚠️ 데이터 삭제 실패: {str(e)}")

def update_api_key_usage(turso_url, turso_token, api_key, quota_used, has_error=False):
    """API 키 사용 정보 업데이트"""
    try:
        current_time = datetime.now().isoformat()
        error_increment = 1 if has_error else 0
        
        sql = """
        UPDATE api_key_management 
        SET 
            used_quota = COALESCE(used_quota, 0) + ?,
            last_used = ?,
            error_count = error_count + ?,
            test_datetime = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE api_key = ?
        """
        
        execute_turso_query(
            turso_url, turso_token, sql,
            [quota_used, current_time, error_increment, current_time, api_key]
        )
    
    except Exception as e:
        print(f"⚠️ API 키 정보 업데이트 실패: {str(e)}")

def main():
    """메인 함수"""
    print("="*70)
    print("🎬 글로벌 핫데이터 수집기 시작")
    print("="*70)
    
    # Step 1: 부트스트랩 Turso 정보 로드
    bootstrap_url, bootstrap_token = bootstrap_turso_credentials()
    if not bootstrap_url or not bootstrap_token:
        print("\n❌ 부트스트랩 실패. 프로그램 종료")
        return
    
    # Step 2: DB에서 모든 인증 정보 로드
    turso_settings, google_service_account, api_keys = load_all_credentials_from_db(
        bootstrap_url, bootstrap_token
    )
    if not turso_settings or not google_service_account or not api_keys:
        print("\n❌ DB에서 인증 정보 로드 실패. 프로그램 종료")
        return
    
    # Step 3: 최종 Turso 정보 결정
    final_turso_url, final_turso_token = get_final_turso_credentials(
        turso_settings, bootstrap_url, bootstrap_token
    )
    
    # Step 4: Google Sheets 인증
    print("\n🔐 Step 4: Google Sheets 인증")
    try:
        credentials = service_account.Credentials.from_service_account_info(
            google_service_account,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        gc = gspread.authorize(credentials)
        print("✅ Google Sheets 인증 성공")
    
    except Exception as e:
        print(f"❌ Google Sheets 인증 실패: {str(e)}")
        return
    
    # Step 5: DB에서 국가, 카테고리 로드
    print("\n📋 Step 5: DB에서 설정 로드")
    countries = load_countries_from_db(final_turso_url, final_turso_token)
    categories = load_categories_from_db(final_turso_url, final_turso_token)
    
    if not countries or not categories or not api_keys:
        print("\n❌ 필수 설정이 부족합니다. 프로그램 종료")
        return
    
    # Step 6: 기존 데이터 삭제
    clear_hot_data_table(final_turso_url, final_turso_token)
    
    # Step 7: 수집 계획 표시
    print("\n🎯 Step 6: 수집 계획")
    print(f"   📌 API 키: {len(api_keys)}개")
    for key_info in api_keys:
        print(f"      - {key_info['name']} ({key_info['status']})")
    
    print(f"\n   🌍 국가: {len(countries)}개")
    print(f"   📂 카테고리: {len(categories)}개")
    print(f"   🔢 총 조합 수: {len(api_keys) * len(countries) * len(categories)}")
    
    # API 키 사용 테스트 (할당량 1 소비로 업데이트)
    if api_keys:
        test_api_key = api_keys[0]['key']
        update_api_key_usage(final_turso_url, final_turso_token, test_api_key, 1, False)
        print(f"\n✅ API 키 업데이트 테스트 완료 (할당량 +1 소비)")
    
    print("\n" + "="*70)
    print("✅ 모든 정보 로드 및 준비 완료!")
    print("="*70)
    print("📝 다음 단계:")
    print("   - YouTube API 호출 로직 구현")
    print("   - global_hot_data 테이블에 데이터 삽입")
    print("   - GitHub Actions 자동화")
    print("="*70)

if __name__ == '__main__':
    main()
