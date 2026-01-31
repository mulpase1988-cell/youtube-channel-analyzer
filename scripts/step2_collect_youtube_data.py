# scripts/step2_collect_youtube_data.py
"""
Step 2: YouTube 데이터 수집
- Step 1의 channel_ids.json 읽기
- YouTube API + RSS로 채널 정보 수집
- youtube_data.json 저장
"""

import os
import json
import logging
import time
import feedparser
from datetime import datetime, timezone
from dateutil import parser as dateutil_parser

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# 설정
# ============================================================================

DATA_DIR = 'data'
CHANNEL_IDS_FILE = os.path.join(DATA_DIR, 'channel_ids.json')
YOUTUBE_DATA_FILE = os.path.join(DATA_DIR, 'youtube_data.json')

API_TAB_NAME = 'API_키_관리'
SHEET_NAME = '유튜브보물창고_테스트'

# 국가 매핑
COUNTRY_MAP = {
    'KR': '한국', 'JP': '일본', 'US': '미국', 'GB': '영국',
    'DE': '독일', 'FR': '프랑스', 'VN': '베트남', 'TH': '태국',
    'ID': '인도네시아', 'IN': '인도', 'BR': '브라질', 'MX': '멕시코',
    'CA': '캐나다', 'AU': '호주', 'RU': '러시아', 'TR': '터키',
    'ES': '스페인', 'IT': '이탈리아', 'TW': '대만', 'HK': '홍콩', 'PH': '필리핀'
}

# 카테고리 매핑
CATEGORY_MAP = {
    '1': '영화', '2': '자동차', '10': '음악', '15': '반려동물',
    '17': '스포츠', '18': '단편영상', '19': '여행', '20': '게임',
    '21': '블로거', '22': '인물', '23': '코미디', '24': '엔터테인먼트',
    '25': '뉴스', '26': '교육', '27': '과학', '28': '기술', '29': '사회'
}

# ============================================================================
# API 키 로드
# ============================================================================

def load_api_keys_from_google_sheets():
    """Google Sheets에서 API 키 로드"""
    import gspread
    from google.oauth2.service_account import Credentials
    
    service_account_json = os.getenv('GOOGLE_SERVICE_ACCOUNT')
    if not service_account_json:
        logger.error("❌ GOOGLE_SERVICE_ACCOUNT 환경변수가 없습니다")
        return []
    
    try:
        service_account_info = json.loads(service_account_json)
    except json.JSONDecodeError as e:
        logger.error(f"❌ JSON 파싱 실패: {e}")
        return []
    
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    credentials = Credentials.from_service_account_info(
        service_account_info,
        scopes=scopes
    )
    
    gc = gspread.authorize(credentials)
    
    try:
        spreadsheet = gc.open(SHEET_NAME)
        worksheet_api = spreadsheet.worksheet(API_TAB_NAME)
        logger.info(f"✅ '{API_TAB_NAME}' 워크시트 연결 성공")
    except Exception as e:
        logger.error(f"❌ Google Sheets 연결 실패: {e}")
        return []
    
    try:
        all_values = worksheet_api.get_all_values()
        
        if len(all_values) < 4:
            logger.warning("⚠️ API 키 데이터가 없습니다")
            return []
        
        headers = all_values[2]
        
        try:
            idx_name = headers.index('키 이름')
            idx_key = headers.index('API 키')
            idx_status = headers.index('활성화')
        except ValueError as e:
            logger.error(f"❌ 필수 컬럼을 찾을 수 없습니다: {e}")
            return []
        
        api_keys = []
        
        for row_idx, row in enumerate(all_values[3:], start=4):
            if len(row) <= max(idx_name, idx_key, idx_status):
                continue
            
            key_name = row[idx_name].strip() if len(row) > idx_name else ""
            key_value = row[idx_key].strip() if len(row) > idx_key else ""
            status = row[idx_status].strip() if len(row) > idx_status else ""
            
            if key_value and status.upper() in ['TRUE', 'YES', 'O', '활성화', '사용']:
                api_keys.append({
                    'name': key_name,
                    'key': key_value
                })
                masked_key = key_value[:10] + '...' + key_value[-5:]
                logger.info(f"   ✓ {key_name}: {masked_key}")
        
        logger.info(f"✅ API 키 {len(api_keys)}개 로드 완료")
        return api_keys
    
    except Exception as e:
        logger.error(f"❌ API 키 로드 실패: {e}")
        return []

# ============================================================================
# RSS 피드 파싱
# ============================================================================

def parse_rss_feed(channel_id, max_videos=15):
    """YouTube RSS 피드에서 최근 영상 추출"""
    
    rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    
    try:
        feed = feedparser.parse(rss_url)
        
        if not feed.entries:
            return []
        
        videos = []
        for entry in feed.entries[:max_videos]:
            try:
                video_id = entry.yt_videoid if hasattr(entry, 'yt_videoid') else None
                if not video_id and 'id' in entry:
                    video_id = entry.id.split(':')[-1]
                
                published_str = entry.published if hasattr(entry, 'published') else None
                published_at = None
                
                if published_str:
                    try:
                        published_at = dateutil_parser.parse(published_str)
                        if published_at.tzinfo is None:
                            published_at = published_at.replace(tzinfo=timezone.utc)
                    except:
                        pass
                
                videos.append({
                    'video_id': video_id,
                    'title': entry.title if hasattr(entry, 'title') else '',
                    'published_at': published_at
                })
            except Exception as e:
                logger.warning(f"⚠️ RSS 항목 파싱 실패: {e}")
                continue
        
        return videos
    
    except Exception as e:
        logger.warning(f"⚠️ RSS 피드 파싱 실패: {e}")
        return []

# ============================================================================
# 채널 데이터 수집
# ============================================================================

def get_channel_data(channel_id, api_key):
    """YouTube API로 채널 데이터 수집"""
    
    result = {
        'channel_id': channel_id,
        'channel_name': '',
        'handle': '',
        'country': '한국',
        'subscribers': 0,
        'video_count': 0,
        'total_views': 0,
        'first_upload': '',
        'latest_upload': '',
        'yt_category': '미분류',
        'video_links': ['', '', '', '', ''],
        'views_5': 0,
        'views_10': 0,
        'views_20': 0,
        'views_30': 0,
        'views_5d': 0,
        'views_10d': 0,
        'views_15d': 0,
        'count_5d': 0,
        'count_10d': 0,
        'operation_days': 0,
        'collect_date': datetime.now(timezone.utc).strftime('%Y-%m-%d')
    }
    
    try:
        youtube = build('youtube', 'v3', developerKey=api_key)
        
        # 채널 정보 조회
        logger.info(f"   📡 채널 정보 조회 중...")
        channel_response = youtube.channels().list(
            part='snippet,statistics,contentDetails',
            id=channel_id
        ).execute()
        
        if not channel_response.get('items'):
            logger.error(f"   ❌ 채널 정보를 찾을 수 없습니다")
            return None
        
        channel_info = channel_response['items'][0]
        snippet = channel_info['snippet']
        statistics = channel_info['statistics']
        
        result['channel_name'] = snippet.get('title', '')
        result['handle'] = snippet.get('customUrl', '')
        
        country_code = snippet.get('country', '').strip()
        if country_code:
            result['country'] = COUNTRY_MAP.get(country_code, country_code)
        
        result['subscribers'] = int(statistics.get('subscriberCount', 0))
        result['video_count'] = int(statistics.get('videoCount', 0))
        result['total_views'] = int(statistics.get('viewCount', 0))
        
        logger.info(f"   ✓ 채널: {result['channel_name']}")
        logger.info(f"   ✓ 구독자: {result['subscribers']:,} | 영상: {result['video_count']:,} | 조회수: {result['total_views']:,}")
        
        # 채널 개설일
        channel_created = snippet.get('publishedAt', '')
        
        # RSS 피드에서 영상 정보 수집
        logger.info(f"   📥 RSS 피드 수집 중...")
        rss_videos = parse_rss_feed(channel_id, max_videos=15)
        logger.info(f"   ✓ RSS에서 {len(rss_videos)}개 영상 수집")
        
        # API에서 플레이리스트 영상 수집
        uploads_playlist_id = channel_info['contentDetails']['relatedPlaylists']['uploads']
        
        try:
            logger.info(f"   📥 업로드 플레이리스트 수집 중...")
            playlist_response = youtube.playlistItems().list(
                part='contentDetails',
                playlistId=uploads_playlist_id,
                maxResults=30
            ).execute()
            
            api_videos = []
            for item in playlist_response.get('items', []):
                try:
                    video_id = item['contentDetails']['videoId']
                    api_videos.append(video_id)
                except:
                    continue
            
            logger.info(f"   ✓ API에서 {len(api_videos)}개 영상 수집")
        
        except HttpError as e:
            if e.resp.status == 404:
                logger.warning(f"   ⚠️ 업로드 플레이리스트 없음 (Shorts 채널?)")
                api_videos = []
            else:
                raise
        
        # 영상 정보 수집 (RSS + API)
        all_video_ids = [v['video_id'] for v in rss_videos if v['video_id']] + api_videos
        all_video_ids = list(dict.fromkeys(all_video_ids))[:30]  # 중복 제거, 최대 30개
        
        if all_video_ids:
            logger.info(f"   📺 영상 정보 조회 중 ({len(all_video_ids)}개)...")
            videos_response = youtube.videos().list(
                part='statistics,snippet',
                id=','.join(all_video_ids)
            ).execute()
            
            # 카테고리 설정
            if videos_response.get('items'):
                try:
                    first_category_id = videos_response['items'][0]['snippet'].get('categoryId', '')
                    result['yt_category'] = CATEGORY_MAP.get(first_category_id, '미분류')
                except:
                    pass
            
            # 영상 링크 저장
            for i, item in enumerate(videos_response.get('items', [])[:5]):
                try:
                    result['video_links'][i] = f"https://www.youtube.com/watch?v={item['id']}"
                except:
                    pass
            
            # 조회수 계산
            view_data = {}
            now = datetime.now(timezone.utc)
            
            for video in videos_response.get('items', []):
                try:
                    video_id = video['id']
                    view_count = int(video['statistics'].get('viewCount', 0))
                    published_str = video['snippet'].get('publishedAt', '')
                    
                    published_at = None
                    if published_str:
                        try:
                            published_at = datetime.fromisoformat(published_str.replace('Z', '+00:00'))
                        except:
                            pass
                    
                    view_data[video_id] = {
                        'views': view_count,
                        'published_at': published_at
                    }
                except:
                    continue
            
            # 일자별 조회수 합계
            views_list = [v['views'] for v in view_data.values()]
            result['views_5'] = sum(views_list[:5])
            result['views_10'] = sum(views_list[:10])
            result['views_20'] = sum(views_list[:20])
            result['views_30'] = sum(views_list[:30])
            
            views_5d = []
            views_10d = []
            views_15d = []
            dates = []
            
            for video_id, data in view_data.items():
                pub_at = data['published_at']
                if not pub_at:
                    continue
                
                dates.append(pub_at)
                days_ago = (now - pub_at).days
                
                if days_ago <= 5:
                    views_5d.append(data['views'])
                if days_ago <= 10:
                    views_10d.append(data['views'])
                if days_ago <= 15:
                    views_15d.append(data['views'])
            
            result['views_5d'] = sum(views_5d)
            result['views_10d'] = sum(views_10d)
            result['views_15d'] = sum(views_15d)
            result['count_5d'] = len(views_5d)
            result['count_10d'] = len(views_10d)
            
            # 최초/최근 업로드
            if dates:
                result['latest_upload'] = max(dates).strftime('%Y-%m-%d')
                result['first_upload'] = min(dates).strftime('%Y-%m-%d')
                result['operation_days'] = (now - min(dates)).days
            elif channel_created:
                result['first_upload'] = channel_created[:10]
                try:
                    created_date = datetime.fromisoformat(channel_created.replace('Z', '+00:00'))
                    result['operation_days'] = (now - created_date).days
                except:
                    pass
            
            logger.info(f"   ✅ 5일: {result['views_5d']:,}회 ({result['count_5d']}개)")
            logger.info(f"   ✅ 10일: {result['views_10d']:,}회 ({result['count_10d']}개)")
            logger.info(f"   ✅ 15일: {result['views_15d']:,}회")
        
        return result
    
    except Exception as e:
        logger.error(f"   ❌ 데이터 수집 실패: {e}")
        return None

# ============================================================================
# Step 2 메인 함수
# ============================================================================

def process_step2():
    """Step 2: YouTube 데이터 수집"""
    
    logger.info("=" * 80)
    logger.info("🚀 Step 2: YouTube 데이터 수집 시작")
    logger.info("=" * 80)
    
    try:
        # [1/4] channel_ids.json 로드
        logger.info("\n[1/4] channel_ids.json 로드 중...")
        
        if not os.path.exists(CHANNEL_IDS_FILE):
            logger.error(f"❌ {CHANNEL_IDS_FILE} 파일이 없습니다")
            logger.error("   Step 1을 먼저 실행해주세요")
            return
        
        with open(CHANNEL_IDS_FILE, 'r', encoding='utf-8') as f:
            channel_ids_data = json.load(f)
        
        logger.info(f"✅ {len(channel_ids_data)}개 채널 로드 완료")
        
        # [2/4] API 키 로드
        logger.info("\n[2/4] API 키 로드 중...")
        api_keys = load_api_keys_from_google_sheets()
        
        if not api_keys:
            logger.error("❌ API 키를 로드할 수 없습니다")
            return
        
        api_key = api_keys[0]['key']
        
        # [3/4] 데이터 수집
        logger.info("\n[3/4] 채널별 데이터 수집 중...")
        youtube_data = []
        success_count = 0
        fail_count = 0
        
        for idx, channel_info in enumerate(channel_ids_data, 1):
            channel_id = channel_info['channel_id']
            channel_name = channel_info['channel_name']
            
            logger.info(f"\n▶ [{idx}/{len(channel_ids_data)}] {channel_name}")
            
            data = get_channel_data(channel_id, api_key)
            
            if data:
                youtube_data.append(data)
                success_count += 1
            else:
                fail_count += 1
            
            # API 레이트 리미트 대비
            time.sleep(1)
        
        # [4/4] 결과 저장
        logger.info("\n[4/4] 결과 저장 중...")
        os.makedirs(DATA_DIR, exist_ok=True)
        
        with open(YOUTUBE_DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(youtube_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"✅ 결과 저장: {YOUTUBE_DATA_FILE}")
        
        # 요약
        logger.info("\n" + "=" * 80)
        logger.info("📊 Step 2 완료 요약")
        logger.info("=" * 80)
        logger.info(f"✅ 수집 성공: {success_count}개")
        logger.info(f"❌ 수집 실패: {fail_count}개")
        logger.info(f"📁 저장 파일: {YOUTUBE_DATA_FILE}")
        logger.info("=" * 80)
    
    except Exception as e:
        logger.error(f"\n❌ Step 2 실패: {e}", exc_info=True)

# ============================================================================
# 실행
# ============================================================================

if __name__ == '__main__':
    process_step2()
