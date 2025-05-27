# db_utils.py
import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import datetime

def get_db_engine():
    """DB 연결을 위한 SQLAlchemy 엔진 생성"""
    load_dotenv()
    
    db_host = os.getenv("DB_HOST")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")
    db_port = os.getenv("DB_PORT")

    if not all([db_host, db_user, db_password, db_name, db_port]):
        raise ValueError("DB 연결 정보가 .env 파일에 올바르게 설정되지 않았습니다.")

    engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")
    return engine

def save_win_probabilities(win_probability_df):
    """승률 예측 DataFrame을 DB에 저장"""
    try:
        engine = get_db_engine()
        
        # DataFrame을 DB에 저장하기 좋은 긴 형식(long format)으로 변환
        long_format_df = win_probability_df.stack().reset_index()
        long_format_df.columns = ['team1', 'team2', 'win_probability']
        
        # 같은 팀 간의 대결('-' 값)은 제외
        long_format_df = long_format_df[long_format_df['win_probability'] != '-'].copy()
        
        # 데이터 타입 변환 및 예측 날짜 추가
        long_format_df['win_probability'] = pd.to_numeric(long_format_df['win_probability'])
        long_format_df['prediction_date'] = datetime.date.today()

        # DB에 'kbo_win_predictions' 테이블로 저장 (기존 테이블이 있다면 덧붙임)
        long_format_df.to_sql('kbo_win_predictions', con=engine, if_exists='append', index=False)
        
        print(f"✅ {datetime.datetime.now()}: 총 {len(long_format_df)} 건의 예측 결과를 DB에 성공적으로 저장했습니다.")

    except Exception as e:
        print(f"❌ DB 저장 중 오류 발생: {e}")
