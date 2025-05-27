# db_utils.py
import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv # 이 라인은 이제 필요 없을 수 있지만, 다른 용도로 load_dotenv()를 쓸 수도 있으므로 유지
import datetime

def get_db_engine():
    """DB 연결을 위한 SQLAlchemy 엔진 생성"""
    #DB URI를 직접 명시
    db_uri = 'mysql+pymysql://root:dugout2025!!@dugout-dev.cn6mm486utfi.ap-northeast-2.rds.amazonaws.com:3306/dugoutDB?charset=utf8'

    # 디버깅을 위해 print 추가 
    print(f"DEBUG: Using DB URI: {db_uri.split('//')[0]}//****:****@{db_uri.split('@')[1]}") # 비밀번호는 가립니다.

    if not db_uri:
        raise ValueError("DB URI가 올바르게 설정되지 않았습니다.")

    engine = create_engine(db_uri)
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
