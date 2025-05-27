# tasks.py
from crawler import crawl_hitter_data, crawl_pitcher_data, load_historical_data
from data_processor import process_hitter_data, process_pitcher_data
from predictor import generate_win_probability_df
from db_utils import save_win_probabilities
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

        # 3. 데이터 처리
        all_hitter_data = process_hitter_data(hitter_data_2025, hitter_data_his)
        all_pitcher_data = process_pitcher_data(pitcher_data_2025, pitcher_data_his)

        # 4. 승률 예측
        win_probability_df = generate_win_probability_df(all_hitter_data, all_pitcher_data)

        # 5. DB에 저장
        save_win_probabilities(win_probability_df)

    except Exception as e:
        print(f"❌ 일일 작업 실행 중 심각한 오류 발생: {e}")
