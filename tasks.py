# tasks.py
from crawler import crawl_hitter_data, crawl_pitcher_data, load_historical_data
from data_processor import process_hitter_data, process_pitcher_data
from predictor import generate_win_probability_df
from db_utils import save_win_probabilities, save_team_rankings, save_hitter_data, save_pitcher_data
import datetime
import pytz # pytz 임포트 추가 (서울 시간대 사용을 위해)


def run_daily_prediction_job():
    """매일 실행될 예측 및 DB 저장 작업"""
    # 서울 시간대로 현재 시각 가져오기
    seoul_timezone = pytz.timezone('Asia/Seoul')
    current_seoul_time = datetime.datetime.now(seoul_timezone)
    print(f"⏰ {current_seoul_time.strftime('%Y-%m-%d %H:%M:%S')}: 일일 예측 및 DB 적재 작업을 시작합니다.")

    try:
        # 1. 데이터 크롤링 (2025년 최신 데이터)
        hitter_data_2025 = crawl_hitter_data()
        pitcher_data_2025 = crawl_pitcher_data()

        # 2. 역대 데이터 로드
        hitter_data_his, pitcher_data_his = load_historical_data()

        # 3. 데이터 처리 (2025년 데이터와 역대 데이터 합치기 및 추가 처리)
        all_hitter_data = process_hitter_data(hitter_data_2025, hitter_data_his)
        all_pitcher_data = process_pitcher_data(pitcher_data_2025, pitcher_data_his)

        # 3.5. 모든 타자/투수 데이터 (2025년 + 과거 연도) DB에 적재
        # 이전에 2025년 데이터만 저장하던 것을 all_hitter_data, all_pitcher_data로 변경
        print("➡️ 모든 타자 데이터 (과거 + 2025년) DB 적재를 시작합니다.")
        save_hitter_data(all_hitter_data)
        print("➡️ 모든 투수 데이터 (과거 + 2025년) DB 적재를 시작합니다.")
        save_pitcher_data(all_pitcher_data)

        # 4. 승률 예측 및 팀 순위 예측 (all_hitter_data와 all_pitcher_data 사용)
        win_probability_df, predicted_team_rankings_df = generate_win_probability_df(all_hitter_data, all_pitcher_data)

        # 5. 예측 결과 DB에 적재
        print("➡️ 승률 예측 결과 DB 적재를 시작합니다.")
        save_win_probabilities(win_probability_df)
        print("➡️ 팀 순위 예측 결과 DB 적재를 시작합니다.")
        save_team_rankings(predicted_team_rankings_df)

        print(f"✅ {current_seoul_time.strftime('%Y-%m-%d %H:%M:%S')}: 일일 예측 및 DB 적재 작업이 성공적으로 완료되었습니다.")

    except Exception as e:
        print(f"❌ {current_seoul_time.strftime('%Y-%m-%d %H:%M:%S')}: 일일 예측 및 DB 적재 작업 중 오류 발생: {e}")
