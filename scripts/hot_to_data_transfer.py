# ========================================
# 핫데이터 → 데이터 이전 - GitHub Actions 버전
# ========================================

import gspread
import os
import tempfile
from google.oauth2.service_account import Credentials
from datetime import datetime

# ========================================
# 환경변수에서 인증 정보 로드
# ========================================
SERVICE_ACCOUNT_JSON = os.environ.get('GOOGLE_SERVICE_ACCOUNT')
if not SERVICE_ACCOUNT_JSON:
    raise Exception("❌ GOOGLE_SERVICE_ACCOUNT 환경변수가 설정되지 않았습니다")

# JSON을 임시 파일로 저장
with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
    f.write(SERVICE_ACCOUNT_JSON)
    SERVICE_ACCOUNT_FILE = f.name

SHEET_NAME = os.environ.get('SHEET_NAME', '유튜브보물창고_테스트')

# ========================================
# 메인 이전 함수
# ========================================
def transfer_to_data_no_cat1():
    """글로벌_핫데이터에서 새 채널을 데이터로 이전"""
    print("=" * 60)
    print("🔄 핫데이터 → 데이터 이전")
    print("=" * 60)
    print(f"📊 '글로벌_핫데이터' → '데이터' 이전을 시작합니다.")
    print("📝 분류1은 비워두고, YT카테고리에 입력합니다.\n")
    
    try:
        # 1. 시트 연결
        print("📊 Google Sheets 연결 중...")
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
        client = gspread.authorize(creds)
        spreadsheet = client.open(SHEET_NAME)
        print(f"✅ '{SHEET_NAME}' 연결 완료\n")
        
        # 2. 워크시트 로드
        print("📂 워크시트 로드 중...")
        ws_hot = spreadsheet.worksheet('글로벌_핫데이터')
        ws_data = spreadsheet.worksheet('데이터')
        print("✅ '글로벌_핫데이터' 로드 완료")
        print("✅ '데이터' 로드 완료\n")

        # 3. 데이터 및 헤더 로드
        print("🔍 기존 데이터 분석 중...")
        hot_data = ws_hot.get_all_records()
        data_all = ws_data.get_all_values()
        data_header = data_all[0]
        
        print(f"📊 글로벌_핫데이터: {len(hot_data)}개 행")
        print(f"📊 데이터 기존: {len(data_all) - 1}개 행\n")
        
        # 'channel_id ' 컬럼 인덱스 확인 (24번째 컬럼, 인덱스 23)
        try:
            cid_idx = data_header.index('channel_id ')
            print(f"✅ channel_id 컬럼 찾음: {cid_idx + 1}번째 컬럼\n")
        except ValueError:
            print("❌ '데이터' 시트에서 'channel_id ' 컬럼명을 찾을 수 없습니다.")
            print("💡 컬럼명을 확인하세요. 공백이 포함되어 있을 수 있습니다.\n")
            return

        # 기존 데이터에 있는 채널 ID 추출
        existing_cids = set([row[cid_idx] for row in data_all[1:] if len(row) > cid_idx and row[cid_idx]])
        print(f"🔍 기존 채널 ID: {len(existing_cids)}개\n")
        
        new_rows = []
        added_this_session = set()

        # 4. 데이터 매핑 진행 (총 33개 컬럼 구조 - AG열까지)
        print("=" * 60)
        print("🔄 새 채널 검색 및 매핑 중...")
        print("=" * 60)
        
        for idx, row in enumerate(hot_data, 1):
            c_id = str(row.get('채널ID', '')).strip()
            
            # 중복 검사
            if c_id and c_id not in existing_cids and c_id not in added_this_session:
                # 데이터의 33개 컬럼 구조 생성 (A~AG)
                new_entry = [""] * 33
                
                new_entry[0] = row.get('채널명', '')                              # A: 채널명
                new_entry[1] = f"https://www.youtube.com/channel/{c_id}"         # B: URL
                new_entry[2] = row.get('핸들명(@)', '')                          # C: 핸들
                new_entry[3] = row.get('국가', '')                               # D: 국가
               new_entry[4] = '미분류'                                          # E: 분류1 (미분류로 설정)

                # new_entry[5] = ''  # F: 분류2 (수동 입력 - 비워둠)
                # new_entry[6] = ''  # G: 메모 (수동 입력 - 비워둠)
                new_entry[7] = row.get('구독자수', 0)                            # H: 구독자
                # new_entry[8~11] = ''  # I~L: 동영상, 조회수, 최초/최근 업로드 (채널분석기가 채움)
                new_entry[12] = datetime.now().strftime('%Y-%m-%d')              # M: 수집일
                # new_entry[13~20] = ''  # N~U: 조회수 합계, 키워드, 비고, 운영기간, 템플릿 (자동/수동)
                new_entry[17] = row.get('태그', '')                              # R: 키워드 (핫데이터 태그)
                # new_entry[21~22] = ''  # V~W: 5일/10일 기준 (채널분석기가 채움)
                new_entry[23] = c_id                                             # X: channel_id
                # new_entry[24~26] = ''  # Y~AA: 5/10/15일 조회수 합계 (채널분석기가 채움)
                new_entry[27] = row.get('카테고리', '')                          # AB: YT카테고리
                # new_entry[28~32] = ''  # AC~AG: 영상1~5 (채널분석기가 채움)

                new_rows.append(new_entry)
                added_this_session.add(c_id)
                
                # 진행상황 출력
                if len(new_rows) % 10 == 0:
                    print(f"   📌 {len(new_rows)}개 새 채널 발견...")

        # 5. 결과 업데이트
        print("\n" + "=" * 60)
        print("💾 데이터에 저장 중...")
        print("=" * 60)
        
        if new_rows:
            ws_data.append_rows(new_rows, value_input_option='USER_ENTERED')
            
            print("\n" + "=" * 60)
            print("✅ 이전 완료!")
            print("=" * 60)
            print(f"📊 추가된 채널: {len(new_rows)}개")
            print(f"📝 분류1(E열): 비워둠 (수동 입력 대기)")
            print(f"📂 YT카테고리(AB열): 입력 완료")
            print(f"🔄 채널 분석기가 다음 실행 시 상세 정보 수집")
            print("=" * 60)
            
            # 추가된 채널 일부 출력
            print("\n📋 추가된 채널 샘플 (최대 5개):")
            for i, row in enumerate(new_rows[:5], 1):
                print(f"   {i}. {row[0]} ({row[3]}) - {row[27]}")
            
            if len(new_rows) > 5:
                print(f"   ... 외 {len(new_rows) - 5}개")
                
        else:
            print("\n" + "=" * 60)
            print("ℹ️  추가할 새로운 채널이 없습니다.")
            print("=" * 60)
            print("💡 모든 채널이 이미 데이터에 존재합니다.")

    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()

# ========================================
# 실행
# ========================================
if __name__ == '__main__':
    transfer_to_data_no_cat1()
