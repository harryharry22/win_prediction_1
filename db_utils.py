# db_utils.py
import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
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
        long_format_df['prediction_date'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # DB에 저장 (테이블이 없으면 생성, 있으면 추가)
        table_name = 'win_probabilities'
        long_format_df.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f"✅ 승률 예측 결과 {len(long_format_df)}건이 '{table_name}' 테이블에 성공적으로 저장되었습니다.")
    except Exception as e:
        print(f"❌ 승률 예측 결과 DB 저장 중 오류 발생: {e}")

def save_team_rankings(team_rankings_df):
    """팀 순위 예측 DataFrame을 DB에 저장"""
    try:
        engine = get_db_engine()

        # 예측 날짜 추가
        team_rankings_df['prediction_date'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # DB에 저장 (테이블이 없으면 생성, 있으면 추가)
        table_name = 'team_rankings' # 새로운 테이블 이름
        team_rankings_df.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f"✅ 팀 순위 예측 결과 {len(team_rankings_df)}건이 '{table_name}' 테이블에 성공적으로 저장되었습니다.")
    except Exception as e:
        print(f"❌ 팀 순위 예측 결과 DB 저장 중 오류 발생: {e}")
