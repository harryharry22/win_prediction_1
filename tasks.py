# tasks.py
from crawler import crawl_hitter_data, crawl_pitcher_data, load_historical_data
from data_processor import process_hitter_data, process_pitcher_data
from predictor import generate_win_probability_df # generate_win_probability_df만 호출
from db_utils import save_win_probabilities, save_team_rankings, save_hitter_data, save_pitcher_data # 새로 추가된 함수 임포트
import datetime

def run_daily_prediction_job():
    """매일 실행될 예측 및 DB 저장 작업"""
    print(f"⏰ {datetime.datetime.now()}: 일일 예측 및 DB 적재 작업을 시작합니다.")

    try:
        # 1. 데이터 크롤링
        hitter_data_2025 = crawl_hitter_data()
        pitcher_data_2025 = crawl_pitcher_data()

        # 2. 역대 데이터 로드
        hitter_data_his, pitcher_data_his = load_historical_data()

        # 2.5. 크롤링된 2025년 타자/투수 데이터 DB에 적재
        print("➡️ 2025년 타자 데이터 DB 적재를 시작합니다.")
        save_hitter_data(hitter_data_2025)
        print("➡️ 2025년 투수 데이터 DB 적재를 시작합니다.")
        save_pitcher_data(pitcher_data_2025)

        # 3. 데이터 처리
        all_hitter_data = process_hitter_data(hitter_data_2025, hitter_data_his)
        all_pitcher_data = process_pitcher_data(pitcher_data_2025, pitcher_data_his)

        # 4. 승률 예측 및 팀 순위 예측
        # generate_win_probability_df는 이제 두 개의 DataFrame을 반환합니다.
        win_probability_df, predicted_team_rankings_df = generate_win_probability_df(all_hitter_data, all_pitcher_data)

        # 5. 예측 결과 DB에 적재
        print("➡️ 승률 예측 결과 DB 적재를 시작합니다.")
        save_win_probabilities(win_probability_df)
        print("➡️ 팀 순위 예측 결과 DB 적재를 시작합니다.")
        save_team_rankings(predicted_team_rankings_df)

        print(f"✅ {datetime.datetime.now()}: 일일 예측 및 DB 적재 작업이 성공적으로 완료되었습니다.")

    except Exception as e:
        print(f"❌ {datetime.datetime.now()}: 일일 예측 및 DB 적재 작업 중 오류 발생: {e}")
