import os
import json
import requests
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

def main():
    """메인 함수"""
    print("="*70)
    print("🎬 글로벌 핫데이터 수집기 시작")
    print("="*70)
    
    # Step 1: GitHub Secrets에서 직접 로드
    print("\n🔐 Step 1: GitHub Secrets 로드")
    
    turso_url = os.environ.get('TURSO_URL')
    turso_token = os.environ.get('TURSO_TOKEN')
    google_service_account_json = os.environ.get('GOOGLE_SERVICE_ACCOUNT')
    
    if not turso_url or not turso_token or not google_service_account_json:
        print("❌ 필수 Secrets 없음: TURSO_URL, TURSO_TOKEN, GOOGLE_SERVICE_ACCOUNT")
        return
    
    print("✅ 모든 Secrets 로드 완료")
    print(f"   URL: {turso_url[:50]}...")
    
    # Step 2: Google 서비스 계정 파싱
    print("\n🔐 Step 2: Google 서비스 계정 파싱")
    try:
        google_service_account = json.loads(google_service_account_json)
        print("✅ Google 서비스 계정 파싱 완료")
    except Exception as e:
        print(f"❌ Google 서비스 계정 파싱 실패: {str(e)}")
        return
    
    # Step 3: Google Sheets 인증
    print("\n🔐 Step 3: Google Sheets 인증")
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
    
    # Step 4: DB에서 API 키, 국가, 카테고리 로드
    print("\n📋 Step 4: DB에서 설정 로드")
    
    try:
        # API 키
        sql = "SELECT api_key, key_name, status FROM api_key_management WHERE is_active = 'TRUE' ORDER BY number ASC"
        result = execute_turso_query(turso_url, turso_token, sql)
        api_keys = []
        if result and 'results' in result:
            for row in result['results'][0].get('rows', []):
                if row[0] and row[0].startswith('AIza'):
                    api_keys.append({'key': row[0], 'name': row[1], 'status': row[2]})
        print(f"✅ {len(api_keys)}개 API 키 로드")
        
        # 국가
        sql = "SELECT country_name, country_code FROM country_settings WHERE is_active = 'Y' ORDER BY id ASC"
        result = execute_turso_query(turso_url, turso_token, sql)
        countries = []
        if result and 'results' in result:
            for row in result['results'][0].get('rows', []):
                countries.append({'name': row[0], 'code': row[1]})
        print(f"✅ {len(countries)}개 국가 로드")
        
        # 카테고리
        sql = "SELECT category_name, category_id FROM category_config WHERE is_active = 'Y' ORDER BY id ASC"
        result = execute_turso_query(turso_url, turso_token, sql)
        categories = []
        if result and 'results' in result:
            for row in result['results'][0].get('rows', []):
                categories.append({'name': row[0], 'id': row[1]})
        print(f"✅ {len(categories)}개 카테고리 로드")
        
    except Exception as e:
        print(f"❌ DB 로드 실패: {str(e)}")
        return
    
    # Step 5: 수집 계획
    print("\n🎯 Step 5: 수집 계획")
    print(f"   📌 API 키: {len(api_keys)}개")
    print(f"   🌍 국가: {len(countries)}개")
    print(f"   📂 카테고리: {len(categories)}개")
    print(f"   🔢 총 조합: {len(api_keys) * len(countries) * len(categories)}")
    
    print("\n" + "="*70)
    print("✅ 모든 설정 로드 완료!")
    print("="*70)

if __name__ == '__main__':
    main()
